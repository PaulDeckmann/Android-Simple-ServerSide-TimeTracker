package com.example.util.simpletimetracker.data_sync.engine

import com.example.util.simpletimetracker.data_sync.api.ActivityDto
import com.example.util.simpletimetracker.data_sync.api.SyncApi
import com.example.util.simpletimetracker.data_sync.api.SyncPushItem
import com.example.util.simpletimetracker.data_sync.api.SyncPushRequest
import com.example.util.simpletimetracker.data_sync.api.TimeEntryDto
import com.example.util.simpletimetracker.data_sync.db.SyncConflictDBO
import com.example.util.simpletimetracker.data_sync.db.SyncConflictDao
import com.example.util.simpletimetracker.data_sync.db.SyncIdMapDBO
import com.example.util.simpletimetracker.data_sync.db.SyncIdMapDao
import com.example.util.simpletimetracker.data_sync.db.SyncStateDBO
import com.example.util.simpletimetracker.data_sync.db.SyncStateDao
import com.example.util.simpletimetracker.data_sync.keystore.SyncCredentialStore
import com.example.util.simpletimetracker.domain.category.model.Category
import com.example.util.simpletimetracker.domain.timetable.model.SubjectGoal
import com.example.util.simpletimetracker.domain.timetable.model.TimetableDay
import com.example.util.simpletimetracker.domain.timetable.model.TimetableEvent
import com.example.util.simpletimetracker.domain.timetable.model.TimetableEventOverride
import com.example.util.simpletimetracker.domain.timetable.model.TimetableTodo
import com.example.util.simpletimetracker.domain.timetable.repo.SubjectGoalRepo
import com.example.util.simpletimetracker.domain.timetable.repo.TimetableRepo
import com.example.util.simpletimetracker.domain.category.repo.CategoryRepo
import com.example.util.simpletimetracker.domain.category.repo.RecordTypeCategoryRepo
import com.example.util.simpletimetracker.domain.color.model.AppColor
import com.example.util.simpletimetracker.domain.record.interactor.AddRunningRecordMediator
import com.example.util.simpletimetracker.domain.record.interactor.RemoveRunningRecordMediator
import com.example.util.simpletimetracker.domain.record.model.Record
import com.example.util.simpletimetracker.domain.record.model.RecordBase
import com.example.util.simpletimetracker.domain.record.repo.RecordRepo
import com.example.util.simpletimetracker.domain.record.repo.RunningRecordRepo
import com.example.util.simpletimetracker.domain.recordTag.model.RecordTag
import com.example.util.simpletimetracker.domain.recordTag.model.RecordTagValueType
import com.example.util.simpletimetracker.domain.recordTag.repo.RecordTagRepo
import com.example.util.simpletimetracker.domain.recordType.model.RecordType
import com.example.util.simpletimetracker.domain.recordType.repo.RecordTypeRepo
import com.squareup.moshi.Moshi
import com.squareup.moshi.Types
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import javax.inject.Inject
import javax.inject.Singleton
import timber.log.Timber

enum class SyncStatus {
    NOT_CONFIGURED,
    SYNCED,
    PENDING,
    OFFLINE,
    ERROR,
}

/**
 * Mirror based sync engine (v1.2, id mapped, running timers synced).
 *
 * Every local entity has a stable server wide unique sync id stored in
 * sync_id_map (a uuid for new entities; entities synced before the id map
 * existed keep their legacy "a<id>"/"e<id>" ids, seeded by the v3 database
 * migration). Local auto increment ids never reach the server, so several
 * devices can sync against the same account without id collisions.
 *
 * Push: only entities changed since the last successful push are sent, plus
 * tombstones for entities deleted locally since then.
 *
 * Pull: server deletions are applied locally, server edits are applied to
 * entities that did not change locally since the last push, and unknown
 * entities (created on the server or on another device) are imported.
 *
 * Timetable entities (events, per date overrides, free days, todos) and
 * subject hour goals sync as opaque app owned payloads, like categories
 * and tags; referenced local ids (event, activity) travel as sync ids
 * and are resolved on import. The server keeps deletions final: a
 * tombstone is never resurrected by a stale push without a delete.
 *
 * Running timers sync as time entries with a null end time: a timer started
 * on one device appears as running on every device, and stopping it on any
 * device removes the running entry (the stopping device pushes the finished
 * entry). If a timer is stopped on two devices before one of them syncs,
 * both create a finished entry for the same time span.
 *
 * Known limitations: activity renames coming from the server are not
 * applied because the activity repository has no update (local name wins
 * on the next push); record start time changes from the server are logged
 * to the conflict log instead of applied; stop times use the stopping
 * device's clock, so clock skew between devices shifts durations.
 */
@Singleton
class SyncEngine @Inject constructor(
    private val syncApi: SyncApi,
    private val credentialStore: SyncCredentialStore,
    private val recordTypeRepo: RecordTypeRepo,
    private val recordRepo: RecordRepo,
    private val runningRecordRepo: RunningRecordRepo,
    private val categoryRepo: CategoryRepo,
    private val recordTypeCategoryRepo: RecordTypeCategoryRepo,
    private val recordTagRepo: RecordTagRepo,
    private val timetableRepo: TimetableRepo,
    private val subjectGoalRepo: SubjectGoalRepo,
    private val addRunningRecordMediator: AddRunningRecordMediator,
    private val removeRunningRecordMediator: RemoveRunningRecordMediator,
    private val syncStateDao: SyncStateDao,
    private val syncIdMapDao: SyncIdMapDao,
    private val syncConflictDao: SyncConflictDao,
    private val deltaCalculator: SyncDeltaCalculator,
    private val moshi: Moshi,
) {

    private val _status = MutableStateFlow(SyncStatus.NOT_CONFIGURED)

    private val tagListAdapter by lazy {
        moshi.adapter<List<Map<String, Any?>>>(
            Types.newParameterizedType(
                List::class.java,
                Types.newParameterizedType(Map::class.java, String::class.java, Any::class.java),
            ),
        )
    }

    private val stringListAdapter by lazy {
        moshi.adapter<List<String>>(
            Types.newParameterizedType(List::class.java, String::class.java),
        )
    }
    val status: StateFlow<SyncStatus> = _status

    private val syncMutex = Mutex()

    @Volatile
    private var hasPendingSyncRequest = false

    @Volatile
    var isPulling: Boolean = false
        private set

    suspend fun syncNow() {
        if (!credentialStore.isConfigured) {
            _status.value = SyncStatus.NOT_CONFIGURED
            return
        }
        // Only one sync may run at a time: overlapping runs could cancel each
        // other mid apply and leave partially imported data behind.
        // If a sync is already running, remember that a new sync was requested
        // so it immediately runs a follow-up sync to catch changes made in-flight.
        if (!syncMutex.tryLock()) {
            hasPendingSyncRequest = true
            Timber.i("Sync already in progress, queued pending sync request")
            return
        }
        try {
            while (true) {
                hasPendingSyncRequest = false
                _status.value = SyncStatus.PENDING
                try {
                    pushLocalChanges()
                    pullServerChanges()
                    _status.value = SyncStatus.SYNCED
                } catch (e: Exception) {
                    val offline = e is java.io.IOException
                    Timber.e(e, "Sync failed")
                    _status.value = if (offline) SyncStatus.OFFLINE else SyncStatus.ERROR
                    break
                }
                if (!hasPendingSyncRequest) {
                    break
                }
            }
        } finally {
            syncMutex.unlock()
        }
    }

    suspend fun clearConflicts() = syncConflictDao.clear()

    suspend fun getConflicts(): List<SyncConflictDBO> = syncConflictDao.getAll()

    suspend fun getConflictsCount(): Int = syncConflictDao.count()

    private suspend fun pushLocalChanges() {
        // Prune any records shorter than 1 minute (60s) from local database so they are ignored and tombstoned on server
        recordRepo.getAll()
            .filter { it.timeEnded > 0 && (it.timeEnded - it.timeStarted) < MIN_RECORD_DURATION_MILLIS }
            .forEach { shortRecord ->
                Timber.i("Pruning short record %d (< 60s) from local DB", shortRecord.id)
                recordRepo.remove(shortRecord.id)
            }

        val mappings = loadMappings()
        val candidates = buildCandidates(mappings)
        val mirror = syncStateDao.getAll().associate {
            deltaCalculator.key(it.entityType, it.entityId) to it.contentHash
        }
        val delta = deltaCalculator.calculate(candidates, mirror)
        if (delta.upserts.isEmpty() && delta.tombstones.isEmpty()) return

        val items = buildPushItems(delta)
        // Entities the server did not understand (for example timetable
        // types on a server version that predates them) must stay out of
        // the mirror so the next sync pushes them again, instead of
        // silently marking them as synced.
        val retryKeys = mutableSetOf<String>()
        items.chunked(PUSH_BATCH).forEach { batch ->
            val response = syncApi.push(SyncPushRequest(items = batch))
            response.conflicts.forEach { conflict ->
                syncConflictDao.insert(
                    SyncConflictDBO(
                        entityType = conflict.entity_type,
                        entityId = conflict.id,
                        resolution = conflict.resolution,
                        detail = "",
                        createdAt = System.currentTimeMillis(),
                    ),
                )
                if (conflict.resolution == CONFLICT_UNKNOWN_TYPE) {
                    retryKeys.add(deltaCalculator.key(conflict.entity_type, conflict.id))
                }
            }
        }
        // The tombstoned entities no longer exist locally; drop their
        // mappings and remove them from the mirror.
        delta.tombstones.forEach {
            syncIdMapDao.removeBySyncId(it.entityType, it.entityId)
            syncStateDao.remove(it.entityType, it.entityId)
        }
        val now = System.currentTimeMillis()
        val acceptedUpserts = delta.upserts
            .filterNot { deltaCalculator.key(it.entityType, it.entityId) in retryKeys }
            .map {
                SyncStateDBO(
                    entityType = it.entityType,
                    entityId = it.entityId,
                    contentHash = deltaCalculator.contentHash(it.payload),
                    syncedAt = now,
                )
            }
        if (acceptedUpserts.isNotEmpty()) {
            syncStateDao.insertAll(acceptedUpserts)
        }
    }

    private fun buildPushItems(delta: SyncDeltaCalculator.Delta): List<SyncPushItem> {
        val now = nowIso()

        // Running records travel as time entries with a null end time; the
        // server keeps treating them like any other entry.
        fun wireType(entityType: String): String =
            if (entityType == ENTITY_RUNNING_RECORD) ENTITY_TIME_ENTRY else entityType

        val upserts = delta.upserts.map { candidate ->
            SyncPushItem(
                entity_type = wireType(candidate.entityType),
                data = candidate.payload + ("updated_at" to now),
            )
        }
        val tombstones = delta.tombstones.map { candidate ->
            SyncPushItem(
                entity_type = wireType(candidate.entityType),
                data = mapOf(
                    "id" to candidate.entityId,
                    "updated_at" to now,
                    "deleted_at" to now,
                ),
            )
        }
        return upserts + tombstones
    }

    private suspend fun buildCandidates(mappings: IdMappings): List<SyncDeltaCalculator.Candidate> {
        val categoryCandidates = categoryRepo.getAll().map { category ->
            val syncId = mappings.syncIdOf(ENTITY_CATEGORY, category.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_CATEGORY,
                entityId = syncId,
                payload = categoryPayloadContent(
                    syncId = syncId,
                    name = category.name,
                    color = category.color.colorInt,
                    colorId = category.color.colorId,
                    note = category.note,
                ),
            )
        }
        val tagCandidates = recordTagRepo.getAll().map { tag ->
            val syncId = mappings.syncIdOf(ENTITY_RECORD_TAG, tag.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_RECORD_TAG,
                entityId = syncId,
                payload = tagPayloadContent(
                    syncId = syncId,
                    name = tag.name,
                    icon = tag.icon,
                    color = tag.color.colorInt,
                    colorId = tag.color.colorId,
                    iconColorSource = tag.iconColorSource,
                    note = tag.note,
                    archived = tag.archived,
                    valueType = tag.valueType.name,
                    valueSuffix = tag.valueSuffix,
                ),
            )
        }
        val typeCandidates = recordTypeRepo.getAll().map { type ->
            val syncId = mappings.syncIdOf(ENTITY_ACTIVITY, type.id)
            val categorySyncIds = recordTypeCategoryRepo.getCategoryIdsByType(type.id)
                .map { mappings.syncIdOf(ENTITY_CATEGORY, it) }
                .sorted()
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_ACTIVITY,
                entityId = syncId,
                payload = type.toPayloadContent(syncId, categorySyncIds),
            )
        }
        val recordCandidates = recordRepo.getAll()
            .filter { (it.timeEnded - it.timeStarted) >= MIN_RECORD_DURATION_MILLIS }
            .mapNotNull { record ->
            val syncId = mappings.syncIdOf(ENTITY_TIME_ENTRY, record.id)
            val typeSyncId = mappings.existingSyncId(ENTITY_ACTIVITY, record.typeId)
                ?: return@mapNotNull null.also {
                    Timber.w("Record %s references unknown activity %s", record.id, record.typeId)
                }
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_TIME_ENTRY,
                entityId = syncId,
                payload = record.toPayloadContent(syncId, typeSyncId, tagsPayload(record.tags, mappings)),
            )
        }
        val runningCandidates = runningRecordRepo.getAll().mapNotNull { running ->
            val typeSyncId = mappings.existingSyncId(ENTITY_ACTIVITY, running.id)
                ?: return@mapNotNull null.also {
                    Timber.w("Running record %s references unknown activity", running.id)
                }
            val syncId = mappings.syncIdOf(ENTITY_RUNNING_RECORD, running.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_RUNNING_RECORD,
                entityId = syncId,
                payload = runningPayloadContent(
                    syncId = syncId,
                    typeSyncId = typeSyncId,
                    timeStarted = running.timeStarted,
                    comment = running.comment,
                    tags = tagsPayload(running.tags, mappings),
                ),
            )
        }
        val eventCandidates = timetableRepo.getAllEvents().map { event ->
            val syncId = mappings.syncIdOf(ENTITY_TIMETABLE_EVENT, event.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_TIMETABLE_EVENT,
                entityId = syncId,
                payload = timetableEventPayloadContent(
                    syncId = syncId,
                    event = event,
                    activitySyncId = event.activityTypeId
                        ?.let { mappings.existingSyncId(ENTITY_ACTIVITY, it) },
                ),
            )
        }
        val overrideCandidates = timetableRepo.getAllOverrides().mapNotNull { override ->
            val eventSyncId = mappings.existingSyncId(ENTITY_TIMETABLE_EVENT, override.eventId)
                ?: return@mapNotNull null.also {
                    Timber.w("Timetable override references unknown event %s", override.eventId)
                }
            val syncId = mappings.syncIdOf(ENTITY_TIMETABLE_OVERRIDE, override.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_TIMETABLE_OVERRIDE,
                entityId = syncId,
                payload = timetableOverridePayloadContent(
                    syncId = syncId,
                    override = override,
                    eventSyncId = eventSyncId,
                ),
            )
        }
        val dayCandidates = timetableRepo.getDays().map { day ->
            val syncId = mappings.syncIdOf(ENTITY_TIMETABLE_DAY, day.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_TIMETABLE_DAY,
                entityId = syncId,
                payload = timetableDayPayloadContent(
                    syncId = syncId,
                    day = day,
                ),
            )
        }
        val todoCandidates = timetableRepo.getAllTodos().mapNotNull { todo ->
            val eventSyncId = mappings.existingSyncId(ENTITY_TIMETABLE_EVENT, todo.eventId)
                ?: return@mapNotNull null.also {
                    Timber.w("Timetable todo references unknown event %s", todo.eventId)
                }
            val syncId = mappings.syncIdOf(ENTITY_TIMETABLE_TODO, todo.id)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_TIMETABLE_TODO,
                entityId = syncId,
                payload = timetableTodoPayloadContent(
                    syncId = syncId,
                    todo = todo,
                    eventSyncId = eventSyncId,
                ),
            )
        }
        val subjectGoalCandidates = subjectGoalRepo.getAll().mapNotNull { goal ->
            val activitySyncId = mappings.existingSyncId(ENTITY_ACTIVITY, goal.activityTypeId)
                ?: return@mapNotNull null.also {
                    Timber.w("Subject goal references unknown activity %s", goal.activityTypeId)
                }
            val syncId = mappings.syncIdOf(ENTITY_SUBJECT_GOAL, goal.activityTypeId)
            SyncDeltaCalculator.Candidate(
                entityType = ENTITY_SUBJECT_GOAL,
                entityId = syncId,
                payload = subjectGoalPayloadContent(
                    syncId = syncId,
                    goal = goal,
                    activitySyncId = activitySyncId,
                ),
            )
        }
        return categoryCandidates + tagCandidates + typeCandidates + recordCandidates + runningCandidates +
            eventCandidates + overrideCandidates + dayCandidates + todoCandidates + subjectGoalCandidates
    }

    private suspend fun pullServerChanges() {
        val since = credentialStore.lastSyncMarker.takeIf { it.isNotEmpty() }
        val pulled = syncApi.pull(since)
        val mappings = loadMappings()
        val mirror = syncStateDao.getAll().associateBy {
            deltaCalculator.key(it.entityType, it.entityId)
        }
        isPulling = true
        try {
            // Categories and tags first: activities and entries reference them.
            pulled.categories.forEach { data -> applyServerCategory(data, mirror, mappings) }
            pulled.tags.forEach { data -> applyServerTag(data, mirror, mappings) }
            pulled.activities.forEach { activity -> applyServerActivity(activity, mirror, mappings) }
            pulled.time_entries.forEach { entry -> applyServerEntry(entry, mirror, mappings) }
            // Timetable entities after their referenced activities and in
            // dependency order: events before their overrides and todos.
            pulled.timetable_events.forEach { data -> applyServerTimetableEvent(data, mirror, mappings) }
            pulled.timetable_days.forEach { data -> applyServerTimetableDay(data, mirror, mappings) }
            pulled.timetable_overrides.forEach { data -> applyServerTimetableOverride(data, mirror, mappings) }
            pulled.timetable_todos.forEach { data -> applyServerTimetableTodo(data, mirror, mappings) }
            pulled.subject_goals.forEach { data -> applyServerSubjectGoal(data, mirror, mappings) }
        } finally {
            isPulling = false
        }
        credentialStore.lastSyncMarker = pulled.server_time ?: nowIso()
    }

    private suspend fun applyServerCategory(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_CATEGORY, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                recordTypeCategoryRepo.removeAll(categoryId = localId)
                categoryRepo.remove(localId)
                mappings.forget(ENTITY_CATEGORY, syncId)
                syncStateDao.remove(ENTITY_CATEGORY, syncId)
            }
            return
        }

        if (localId != null) {
            val local = categoryRepo.get(localId) ?: return
            val localHash = deltaCalculator.contentHash(
                categoryPayloadContent(
                    syncId = syncId,
                    name = local.name,
                    color = local.color.colorInt,
                    colorId = local.color.colorId,
                    note = local.note,
                ),
            )
            val mirrorHash = mirror[deltaCalculator.key(ENTITY_CATEGORY, syncId)]?.contentHash
            if (mirrorHash != null && mirrorHash == localHash) {
                val name = data["name"] as? String ?: local.name
                val color = data["color"] as? String ?: local.color.colorInt
                val colorId = (data["color_id"] as? Double)?.toInt() ?: local.color.colorId
                val note = data["note"] as? String ?: local.note
                val updated = local.copy(
                    name = name,
                    color = AppColor(colorId = colorId, colorInt = color),
                    note = note,
                )
                if (updated != local) {
                    categoryRepo.add(updated)
                }
                insertMirror(ENTITY_CATEGORY, syncId, categoryPayloadContent(syncId, name, color, colorId, note))
            }
            return
        }

        val name = data["name"] as? String ?: return
        val color = data["color"] as? String ?: ""
        val colorId = (data["color_id"] as? Double)?.toInt() ?: 0
        val note = data["note"] as? String ?: ""
        Timber.i("Importing category %s from sync", syncId)
        val newLocalId = categoryRepo.add(
            Category(
                name = name,
                color = AppColor(colorId = colorId, colorInt = color),
                note = note,
            ),
        )
        mappings.remember(ENTITY_CATEGORY, newLocalId, syncId)
        insertMirror(ENTITY_CATEGORY, syncId, categoryPayloadContent(syncId, name, color, colorId, note))
    }

    private suspend fun applyServerTimetableEvent(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_TIMETABLE_EVENT, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                timetableRepo.removeEvent(localId)
                mappings.forget(ENTITY_TIMETABLE_EVENT, syncId)
                syncStateDao.remove(ENTITY_TIMETABLE_EVENT, syncId)
            }
            return
        }

        val activitySyncId = data["activity_sync_id"] as? String
        val activityLocalId = activitySyncId?.let { mappings.localIdOf(ENTITY_ACTIVITY, it) }

        if (localId != null) {
            val existing = timetableRepo.getAllEvents().firstOrNull { it.id == localId }
            if (existing != null) {
                val existingActivitySyncId = existing.activityTypeId?.let { mappings.existingSyncId(ENTITY_ACTIVITY, it) }
                val localHash = deltaCalculator.contentHash(
                    timetableEventPayloadContent(syncId, existing, existingActivitySyncId),
                )
                val mirrorHash = mirror[deltaCalculator.key(ENTITY_TIMETABLE_EVENT, syncId)]?.contentHash
                if (mirrorHash != null && mirrorHash == localHash) {
                    val name = data["name"] as? String ?: existing.name
                    val dayOfWeek = (data["day_of_week"] as? Double)?.toInt() ?: existing.dayOfWeek
                    val startTime = (data["start_time"] as? Double)?.toInt() ?: existing.startTime
                    val endTime = (data["end_time"] as? Double)?.toInt() ?: existing.endTime
                    val room = data["room"] as? String ?: existing.room
                    val typeOrdinal = (data["type"] as? Double)?.toInt() ?: existing.type.ordinal
                    val type = TimetableEvent.Type.entries.getOrNull(typeOrdinal) ?: existing.type
                    val comment = data["comment"] as? String ?: existing.comment
                    val updated = existing.copy(
                        name = name,
                        dayOfWeek = dayOfWeek,
                        startTime = startTime,
                        endTime = endTime,
                        room = room,
                        type = type,
                        comment = comment,
                        activityTypeId = activityLocalId,
                    )
                    if (updated != existing) {
                        timetableRepo.addEvent(updated)
                    }
                    insertMirror(ENTITY_TIMETABLE_EVENT, syncId, syncPayloadOf(data))
                }
            }
            return
        }

        val name = data["name"] as? String ?: return
        val typeOrdinal = (data["type"] as? Double)?.toInt() ?: 0
        val type = TimetableEvent.Type.entries.getOrNull(typeOrdinal) ?: TimetableEvent.Type.LECTURE
        Timber.i("Importing timetable event %s from sync", syncId)
        val newLocalId = timetableRepo.addEvent(
            TimetableEvent(
                name = name,
                dayOfWeek = (data["day_of_week"] as? Double)?.toInt() ?: 1,
                startTime = (data["start_time"] as? Double)?.toInt() ?: 0,
                endTime = (data["end_time"] as? Double)?.toInt() ?: 0,
                room = data["room"] as? String ?: "",
                type = type,
                comment = data["comment"] as? String ?: "",
                activityTypeId = activityLocalId,
            ),
        )
        mappings.remember(ENTITY_TIMETABLE_EVENT, newLocalId, syncId)
        insertMirror(ENTITY_TIMETABLE_EVENT, syncId, syncPayloadOf(data))
    }

    private suspend fun applyServerTimetableDay(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_TIMETABLE_DAY, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                timetableRepo.removeDay(localId)
                mappings.forget(ENTITY_TIMETABLE_DAY, syncId)
                syncStateDao.remove(ENTITY_TIMETABLE_DAY, syncId)
            }
            return
        }

        if (localId != null) {
            val existing = timetableRepo.getDays().firstOrNull { it.id == localId }
            if (existing != null) {
                val localHash = deltaCalculator.contentHash(
                    timetableDayPayloadContent(syncId, existing),
                )
                val mirrorHash = mirror[deltaCalculator.key(ENTITY_TIMETABLE_DAY, syncId)]?.contentHash
                if (mirrorHash != null && mirrorHash == localHash) {
                    val freeDay = data["free_day"] as? Boolean ?: existing.freeDay
                    val note = data["note"] as? String ?: existing.note
                    val updated = existing.copy(freeDay = freeDay, note = note)
                    if (updated != existing) {
                        timetableRepo.addDay(updated)
                    }
                    insertMirror(ENTITY_TIMETABLE_DAY, syncId, syncPayloadOf(data))
                }
            }
            return
        }

        val date = data["date"] as? String ?: return
        Timber.i("Importing timetable day %s from sync", syncId)
        val newLocalId = timetableRepo.addDay(
            TimetableDay(
                date = date,
                freeDay = data["free_day"] as? Boolean ?: false,
                note = data["note"] as? String ?: "",
            ),
        )
        mappings.remember(ENTITY_TIMETABLE_DAY, newLocalId, syncId)
        insertMirror(ENTITY_TIMETABLE_DAY, syncId, syncPayloadOf(data))
    }

    private suspend fun applyServerTimetableOverride(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_TIMETABLE_OVERRIDE, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                timetableRepo.removeOverride(localId)
                mappings.forget(ENTITY_TIMETABLE_OVERRIDE, syncId)
                syncStateDao.remove(ENTITY_TIMETABLE_OVERRIDE, syncId)
            }
            return
        }

        val eventSyncId = data["event_sync_id"] as? String ?: return
        val eventLocalId = mappings.localIdOf(ENTITY_TIMETABLE_EVENT, eventSyncId) ?: return

        if (localId != null) {
            val existing = timetableRepo.getAllOverrides().firstOrNull { it.id == localId }
            if (existing != null) {
                val localHash = deltaCalculator.contentHash(
                    timetableOverridePayloadContent(syncId, existing, eventSyncId),
                )
                val mirrorHash = mirror[deltaCalculator.key(ENTITY_TIMETABLE_OVERRIDE, syncId)]?.contentHash
                if (mirrorHash != null && mirrorHash == localHash) {
                    val updated = existing.copy(
                        eventId = eventLocalId,
                        room = data["room"] as? String ?: existing.room,
                        startTime = (data["start_time"] as? Double)?.toInt() ?: existing.startTime,
                        endTime = (data["end_time"] as? Double)?.toInt() ?: existing.endTime,
                        cancelled = data["cancelled"] as? Boolean ?: existing.cancelled,
                        note = data["note"] as? String ?: existing.note,
                    )
                    if (updated != existing) {
                        timetableRepo.addOverride(updated)
                    }
                    insertMirror(ENTITY_TIMETABLE_OVERRIDE, syncId, syncPayloadOf(data))
                }
            }
            return
        }

        val date = data["date"] as? String ?: return
        Timber.i("Importing timetable override %s from sync", syncId)
        val newLocalId = timetableRepo.addOverride(
            TimetableEventOverride(
                date = date,
                eventId = eventLocalId,
                room = data["room"] as? String ?: "",
                startTime = (data["start_time"] as? Double)?.toInt() ?: 0,
                endTime = (data["end_time"] as? Double)?.toInt() ?: 0,
                cancelled = data["cancelled"] as? Boolean ?: false,
                note = data["note"] as? String ?: "",
            ),
        )
        mappings.remember(ENTITY_TIMETABLE_OVERRIDE, newLocalId, syncId)
        insertMirror(ENTITY_TIMETABLE_OVERRIDE, syncId, syncPayloadOf(data))
    }

    private suspend fun applyServerTimetableTodo(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_TIMETABLE_TODO, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                timetableRepo.removeTodo(localId)
                mappings.forget(ENTITY_TIMETABLE_TODO, syncId)
                syncStateDao.remove(ENTITY_TIMETABLE_TODO, syncId)
            }
            return
        }

        val done = data["done"] as? Boolean ?: false

        if (localId != null) {
            val existing = timetableRepo.getAllTodos().firstOrNull { it.id == localId }
            if (existing != null) {
                val eventSyncId = mappings.existingSyncId(ENTITY_TIMETABLE_EVENT, existing.eventId)
                if (eventSyncId != null) {
                    val localHash = deltaCalculator.contentHash(
                        timetableTodoPayloadContent(
                            syncId = syncId,
                            todo = existing,
                            eventSyncId = eventSyncId,
                        ),
                    )
                    val mirrorHash = mirror[deltaCalculator.key(ENTITY_TIMETABLE_TODO, syncId)]?.contentHash
                    if (mirrorHash != null && mirrorHash == localHash) {
                        if (existing.done != done) {
                            timetableRepo.setTodoDone(localId, done)
                        }
                        insertMirror(ENTITY_TIMETABLE_TODO, syncId, syncPayloadOf(data))
                    } else if (mirrorHash != null && mirrorHash != localHash && existing.done != done) {
                        syncConflictDao.insert(
                            SyncConflictDBO(
                                entityType = ENTITY_TIMETABLE_TODO,
                                entityId = syncId,
                                resolution = RESOLUTION_LOCAL_KEPT_NAME,
                                detail = "concurrent edit on todo done: local '${existing.done}' vs server '$done'",
                                createdAt = System.currentTimeMillis(),
                            ),
                        )
                    }
                }
            }
            return
        }

        val eventSyncId = data["event_sync_id"] as? String ?: return
        val eventLocalId = mappings.localIdOf(ENTITY_TIMETABLE_EVENT, eventSyncId) ?: return
        Timber.i("Importing timetable todo %s from sync", syncId)
        val typeOrdinal = (data["type"] as? Double)?.toInt() ?: 0
        val type = TimetableTodo.Type.entries.getOrNull(typeOrdinal) ?: TimetableTodo.Type.GENERAL
        val newLocalId = timetableRepo.addTodo(
            TimetableTodo(
                eventId = eventLocalId,
                date = data["date"] as? String,
                text = data["text"] as? String ?: "",
                done = done,
                type = type,
            ),
        )
        mappings.remember(ENTITY_TIMETABLE_TODO, newLocalId, syncId)
        insertMirror(ENTITY_TIMETABLE_TODO, syncId, syncPayloadOf(data))
    }

    private suspend fun applyServerSubjectGoal(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val activitySyncId = data["activity_sync_id"] as? String ?: return
        val activityLocalId = mappings.localIdOf(ENTITY_ACTIVITY, activitySyncId) ?: return
        val localGoal = subjectGoalRepo.get(activityLocalId)

        if (data["deleted_at"] != null) {
            if (localGoal != null) {
                subjectGoalRepo.remove(activityLocalId)
                mappings.forget(ENTITY_SUBJECT_GOAL, syncId)
                syncStateDao.remove(ENTITY_SUBJECT_GOAL, syncId)
            }
            return
        }

        val targetSeconds = (data["target_seconds"] as? Double)?.toLong() ?: 0L
        val ects = (data["ects"] as? Double)

        if (localGoal != null) {
            val localHash = deltaCalculator.contentHash(
                subjectGoalPayloadContent(
                    syncId = syncId,
                    goal = localGoal,
                    activitySyncId = activitySyncId,
                ),
            )
            val mirrorHash = mirror[deltaCalculator.key(ENTITY_SUBJECT_GOAL, syncId)]?.contentHash
            if (mirrorHash != null && mirrorHash == localHash) {
                subjectGoalRepo.set(
                    SubjectGoal(
                        activityTypeId = activityLocalId,
                        targetSeconds = targetSeconds,
                        ects = ects,
                    ),
                )
                insertMirror(ENTITY_SUBJECT_GOAL, syncId, syncPayloadOf(data))
            } else if (mirrorHash != null && mirrorHash != localHash && (localGoal.targetSeconds != targetSeconds || localGoal.ects != ects)) {
                syncConflictDao.insert(
                    SyncConflictDBO(
                        entityType = ENTITY_SUBJECT_GOAL,
                        entityId = syncId,
                        resolution = RESOLUTION_LOCAL_KEPT_NAME,
                        detail = "concurrent edit on subject goal: local ${localGoal.targetSeconds}s vs server ${targetSeconds}s",
                        createdAt = System.currentTimeMillis(),
                    ),
                )
            }
            return
        }

        Timber.i("Importing subject goal %s from sync", syncId)
        subjectGoalRepo.set(
            SubjectGoal(
                activityTypeId = activityLocalId,
                targetSeconds = targetSeconds,
                ects = ects,
            ),
        )
        mappings.remember(ENTITY_SUBJECT_GOAL, activityLocalId, syncId)
        insertMirror(ENTITY_SUBJECT_GOAL, syncId, syncPayloadOf(data))
    }

    private suspend fun applyServerTag(
        data: Map<String, Any?>,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val syncId = data["id"] as? String ?: return
        val localId = mappings.localIdOf(ENTITY_RECORD_TAG, syncId)

        if (data["deleted_at"] != null) {
            if (localId != null) {
                recordTagRepo.remove(localId)
                mappings.forget(ENTITY_RECORD_TAG, syncId)
                syncStateDao.remove(ENTITY_RECORD_TAG, syncId)
            }
            return
        }

        if (localId != null) {
            val local = recordTagRepo.get(localId) ?: return
            val localHash = deltaCalculator.contentHash(
                tagPayloadContent(
                    syncId = syncId,
                    name = local.name,
                    icon = local.icon,
                    color = local.color.colorInt,
                    colorId = local.color.colorId,
                    iconColorSource = local.iconColorSource,
                    note = local.note,
                    archived = local.archived,
                    valueType = local.valueType.name,
                    valueSuffix = local.valueSuffix,
                ),
            )
            val mirrorHash = mirror[deltaCalculator.key(ENTITY_RECORD_TAG, syncId)]?.contentHash
            if (mirrorHash != null && mirrorHash == localHash) {
                val name = data["name"] as? String ?: local.name
                val icon = data["icon"] as? String ?: local.icon
                val color = data["color"] as? String ?: local.color.colorInt
                val colorId = (data["color_id"] as? Double)?.toInt() ?: local.color.colorId
                val note = data["note"] as? String ?: local.note
                val archived = data["archived"] as? Boolean ?: local.archived
                val valueType = (data["value_type"] as? String)
                    ?.let { type -> RecordTagValueType.entries.firstOrNull { it.name == type } }
                    ?: local.valueType
                val valueSuffix = data["value_suffix"] as? String ?: local.valueSuffix
                val updated = local.copy(
                    name = name,
                    icon = icon,
                    color = AppColor(colorId = colorId, colorInt = color),
                    note = note,
                    archived = archived,
                    valueType = valueType,
                    valueSuffix = valueSuffix,
                )
                if (updated != local) {
                    recordTagRepo.add(updated)
                }
                insertMirror(
                    ENTITY_RECORD_TAG,
                    syncId,
                    tagPayloadContent(
                        syncId = syncId,
                        name = name,
                        icon = icon,
                        color = color,
                        colorId = colorId,
                        iconColorSource = local.iconColorSource,
                        note = note,
                        archived = archived,
                        valueType = valueType.name,
                        valueSuffix = valueSuffix,
                    ),
                )
            }
            return
        }

        val name = data["name"] as? String ?: return
        val valueType = (data["value_type"] as? String)
            ?.let { type -> RecordTagValueType.entries.firstOrNull { it.name == type } }
            ?: RecordTagValueType.NONE
        Timber.i("Importing record tag %s from sync", syncId)
        val newLocalId = recordTagRepo.add(
            RecordTag(
                name = name,
                icon = data["icon"] as? String ?: "",
                color = AppColor(
                    colorId = (data["color_id"] as? Double)?.toInt() ?: 0,
                    colorInt = data["color"] as? String ?: "",
                ),
                iconColorSource = (data["icon_color_source"] as? Double)?.toLong() ?: 0L,
                note = data["note"] as? String ?: "",
                archived = data["archived"] as? Boolean ?: false,
                valueType = valueType,
                valueSuffix = data["value_suffix"] as? String ?: "",
            ),
        )
        mappings.remember(ENTITY_RECORD_TAG, newLocalId, syncId)
        insertMirror(
            ENTITY_RECORD_TAG,
            syncId,
            tagPayloadContent(
                syncId = syncId,
                name = name,
                icon = data["icon"] as? String ?: "",
                color = data["color"] as? String ?: "",
                colorId = (data["color_id"] as? Double)?.toInt() ?: 0,
                iconColorSource = (data["icon_color_source"] as? Double)?.toLong() ?: 0L,
                note = data["note"] as? String ?: "",
                archived = data["archived"] as? Boolean ?: false,
                valueType = valueType.name,
                valueSuffix = data["value_suffix"] as? String ?: "",
            ),
        )
    }

    // The server adds updated_at and deleted_at; the local payload hash
    // never sees them.
    private fun syncPayloadOf(data: Map<String, Any?>): Map<String, Any?> =
        data.filterKeys { it != "updated_at" && it != "deleted_at" }

    private suspend fun insertMirror(
        entityType: String,
        syncId: String,
        payload: Map<String, Any?>,
    ) {
        syncStateDao.insertAll(
            listOf(
                SyncStateDBO(
                    entityType = entityType,
                    entityId = syncId,
                    contentHash = deltaCalculator.contentHash(payload),
                    syncedAt = System.currentTimeMillis(),
                ),
            ),
        )
    }

    private suspend fun applyServerActivity(
        activity: ActivityDto,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        val localId = mappings.localIdOf(ENTITY_ACTIVITY, activity.id)

        if (activity.deleted_at != null) {
            if (localId != null) {
                // The server deleted the activity: remove it locally
                // together with its records and all related mappings.
                recordRepo.getByType(setOf(localId)).forEach { record ->
                    mappings.existingSyncId(ENTITY_TIME_ENTRY, record.id)?.let { recordSyncId ->
                        mappings.forget(ENTITY_TIME_ENTRY, recordSyncId)
                        syncStateDao.remove(ENTITY_TIME_ENTRY, recordSyncId)
                    }
                }
                recordRepo.removeByType(localId)
                recordTypeRepo.remove(localId)
                mappings.forget(ENTITY_ACTIVITY, activity.id)
                syncStateDao.remove(ENTITY_ACTIVITY, activity.id)
            }
            return
        }

        if (localId == null) {
            // Unknown activity: created on the server or another device.
            importActivity(activity, mappings)
            return
        }

        val local = recordTypeRepo.get(localId) ?: return
        val syncId = mappings.existingSyncId(ENTITY_ACTIVITY, localId) ?: return

        val localCategorySyncIds = recordTypeCategoryRepo.getCategoryIdsByType(localId)
            .mapNotNull { mappings.existingSyncId(ENTITY_CATEGORY, it) }
            .sorted()
        val localHash = deltaCalculator.contentHash(local.toPayloadContent(syncId, localCategorySyncIds))
        val mirrorHash = mirror[deltaCalculator.key(ENTITY_ACTIVITY, syncId)]?.contentHash
        if (mirrorHash != null && mirrorHash == localHash) {
            val updatedLocal = local.copy(
                name = activity.name,
                icon = activity.icon,
                color = AppColor(colorId = activity.color_id, colorInt = activity.color),
            )
            if (updatedLocal != local) {
                recordTypeRepo.add(updatedLocal)
            }
            // Apply server side category assignment changes.
            val serverCategoryIds = decodeStringList(activity.category)
                .mapNotNull { mappings.localIdOf(ENTITY_CATEGORY, it) }
            val localCategoryIds = recordTypeCategoryRepo.getCategoryIdsByType(localId)
            val toAdd = (serverCategoryIds - localCategoryIds.toSet()).toList()
            val toRemove = (localCategoryIds - serverCategoryIds.toSet()).toList()
            if (toAdd.isNotEmpty()) recordTypeCategoryRepo.addCategories(localId, toAdd)
            if (toRemove.isNotEmpty()) recordTypeCategoryRepo.removeCategories(localId, toRemove)

            insertMirror(
                ENTITY_ACTIVITY,
                activity.id,
                mapOf(
                    "id" to activity.id,
                    "name" to activity.name,
                    "icon" to activity.icon,
                    "color" to activity.color,
                    "color_id" to activity.color_id,
                    "archived" to activity.archived,
                    "category" to encodeStringList(decodeStringList(activity.category).sorted()),
                ),
            )
        } else if (mirrorHash != null && mirrorHash != localHash && activity.name != local.name) {
            syncConflictDao.insert(
                SyncConflictDBO(
                    entityType = ENTITY_ACTIVITY,
                    entityId = activity.id,
                    resolution = RESOLUTION_LOCAL_KEPT_NAME,
                    detail = "concurrent edit on activity name: local '${local.name}' vs server '${activity.name}'",
                    createdAt = System.currentTimeMillis(),
                ),
            )
        }
        if (activity.archived && !local.hidden) {
            recordTypeRepo.archive(localId)
        } else if (!activity.archived && local.hidden) {
            recordTypeRepo.restore(localId)
        }
    }

    private suspend fun importActivity(
        activity: ActivityDto,
        mappings: IdMappings,
    ) {
        Timber.i("Importing activity %s from sync", activity.id)
        val localId = recordTypeRepo.add(
            RecordType(
                name = activity.name,
                icon = activity.icon,
                // Predefined app colors live in colorId; without it every
                // imported activity would fall back to the same fallback
                // color on the receiving device.
                color = AppColor(colorId = activity.color_id, colorInt = activity.color),
                defaultDuration = 0,
                note = "",
                hidden = activity.archived,
            ),
        )
        mappings.remember(ENTITY_ACTIVITY, localId, activity.id)
        val categoryIds = decodeStringList(activity.category)
            .mapNotNull { mappings.localIdOf(ENTITY_CATEGORY, it) }
        if (categoryIds.isNotEmpty()) recordTypeCategoryRepo.addCategories(localId, categoryIds)
        insertMirror(
            ENTITY_ACTIVITY,
            activity.id,
            mapOf(
                "id" to activity.id,
                "name" to activity.name,
                "icon" to activity.icon,
                "color" to activity.color,
                "color_id" to activity.color_id,
                "archived" to activity.archived,
                "category" to encodeStringList(decodeStringList(activity.category).sorted()),
            ),
        )
    }

    private suspend fun applyServerEntry(
        entry: TimeEntryDto,
        mirror: Map<String, SyncStateDBO>,
        mappings: IdMappings,
    ) {
        // Entries without an end time are running timers started on
        // another device.
        if (entry.ended_at == null) {
            applyServerRunningEntry(entry, mappings)
            return
        }

        val localId = mappings.localIdOf(ENTITY_TIME_ENTRY, entry.id)

        if (entry.deleted_at != null) {
            if (localId != null) {
                recordRepo.remove(localId)
                mappings.forget(ENTITY_TIME_ENTRY, entry.id)
                syncStateDao.remove(ENTITY_TIME_ENTRY, entry.id)
            }
            return
        }

        if (localId == null) {
            // Unknown entry: created on the server or another device.
            importEntry(entry, mappings)
            return
        }

        val local = recordRepo.get(localId) ?: return
        val syncId = mappings.existingSyncId(ENTITY_TIME_ENTRY, localId) ?: return
        val typeSyncId = mappings.existingSyncId(ENTITY_ACTIVITY, local.typeId) ?: return

        val parsed = entry.toParsed() ?: run {
            Timber.w("Skipping time entry %s: unparsable activity id or timestamps", entry.id)
            return
        }
        if (parsed.timeEnded > 0 && (parsed.timeEnded - parsed.timeStarted) < MIN_RECORD_DURATION_MILLIS) {
            Timber.i("Removing local time entry %d: duration is < 60s from sync", localId)
            recordRepo.remove(localId)
            mappings.forget(ENTITY_TIME_ENTRY, entry.id)
            syncStateDao.remove(ENTITY_TIME_ENTRY, entry.id)
            return
        }
        val serverTypeId = mappings.localIdOf(ENTITY_ACTIVITY, entry.activity_id)
            ?: run {
                Timber.w("Skipping time entry %s: unknown activity %s", entry.id, entry.activity_id)
                return
            }
        val mirrorHash = mirror[deltaCalculator.key(ENTITY_TIME_ENTRY, syncId)]?.contentHash
        val localHash = deltaCalculator.contentHash(
            local.toPayloadContent(syncId, typeSyncId, tagsPayload(local.tags, mappings)),
        )
        // Apply the server version only if the local record did not change
        // since the last push; otherwise the local edit wins and is pushed again.
        if (mirrorHash == null || mirrorHash != localHash) return
        val serverTags = localizeTags(parsed.tags, mappings)
        val serverHash = deltaCalculator.contentHash(
            parsed.toPayloadContent(syncId, entry.activity_id, parsed.tags),
        )
        if (serverHash == localHash) return
        applyServerRecord(local, serverTypeId, parsed, serverTags)
        syncStateDao.updateHash(ENTITY_TIME_ENTRY, syncId, serverHash)
    }

    private suspend fun importEntry(
        entry: TimeEntryDto,
        mappings: IdMappings,
    ) {
        val typeLocalId = mappings.localIdOf(ENTITY_ACTIVITY, entry.activity_id)
        if (typeLocalId == null) {
            Timber.w("Cannot import time entry %s: unknown activity %s", entry.id, entry.activity_id)
            return
        }
        val parsed = entry.toParsed() ?: run {
            Timber.w("Skipping time entry %s: unparsable activity id or timestamps", entry.id)
            return
        }
        if (parsed.timeEnded > 0 && (parsed.timeEnded - parsed.timeStarted) < MIN_RECORD_DURATION_MILLIS) {
            Timber.i("Skipping import of time entry %s: duration is < 60s", entry.id)
            return
        }
        Timber.i("Importing time entry %s from sync", entry.id)
        val localId = recordRepo.add(
            Record(
                id = 0, // Let the local database allocate an id.
                typeId = typeLocalId,
                timeStarted = parsed.timeStarted,
                timeEnded = parsed.timeEnded,
                comment = parsed.comment,
                tags = localizeTags(parsed.tags, mappings),
            ),
        )
        if (runningRecordRepo.has(typeLocalId)) {
            val localRunning = runningRecordRepo.get(typeLocalId)
            if (localRunning != null && localRunning.timeStarted <= parsed.timeEnded) {
                Timber.i("Stopping local running timer %d because finished entry was imported from sync", typeLocalId)
                removeRunningRecordMediator.remove(typeId = typeLocalId)
                val runningSyncId = mappings.existingSyncId(ENTITY_RUNNING_RECORD, typeLocalId)
                if (runningSyncId != null) {
                    mappings.forget(ENTITY_RUNNING_RECORD, runningSyncId)
                    syncStateDao.remove(ENTITY_RUNNING_RECORD, runningSyncId)
                }
            }
        }
        mappings.remember(ENTITY_TIME_ENTRY, localId, entry.id)
        syncStateDao.insertAll(
            listOf(
                SyncStateDBO(
                    entityType = ENTITY_TIME_ENTRY,
                    entityId = entry.id,
                    contentHash = deltaCalculator.contentHash(
                        parsed.toPayloadContent(entry.id, entry.activity_id, parsed.tags),
                    ),
                    syncedAt = System.currentTimeMillis(),
                ),
            ),
        )
    }

    private suspend fun applyServerRecord(
        local: Record,
        serverTypeId: Long,
        server: ParsedEntry,
        serverTags: List<RecordBase.Tag>,
    ) {
        if (serverTypeId != local.typeId || server.comment != local.comment || serverTags != local.tags) {
            recordRepo.update(
                recordId = local.id,
                typeId = serverTypeId,
                comment = server.comment,
                tags = serverTags,
            )
        }
        if (server.timeEnded != local.timeEnded) {
            recordRepo.updateTimeEnded(local.id, server.timeEnded)
        }
        if (server.timeStarted != local.timeStarted) {
            // The record repository cannot change a start time; keep the
            // local value and leave a trace in the conflict log.
            syncConflictDao.insert(
                SyncConflictDBO(
                    entityType = ENTITY_TIME_ENTRY,
                    entityId = "e${local.id}",
                    resolution = RESOLUTION_LOCAL_KEPT_START,
                    detail = "server start time was not applied",
                    createdAt = System.currentTimeMillis(),
                ),
            )
        }
    }

    private fun RecordType.toPayloadContent(
        syncId: String,
        categorySyncIds: List<String>,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "name" to name,
        "icon" to icon,
        "color" to color.colorInt,
        "color_id" to color.colorId,
        "archived" to hidden,
        // Stored in the server's free text category column as a json list.
        "category" to encodeStringList(categorySyncIds),
    )

    private fun Record.toPayloadContent(
        syncId: String,
        typeSyncId: String,
        tags: List<Map<String, Any?>>,
    ): Map<String, Any?> = entryPayloadContent(
        syncId = syncId,
        typeSyncId = typeSyncId,
        timeStarted = timeStarted,
        timeEnded = timeEnded,
        comment = comment,
        tags = tags,
    )

    private fun ParsedEntry.toPayloadContent(
        syncId: String,
        typeSyncId: String,
        tags: List<Map<String, Any?>>,
    ): Map<String, Any?> = entryPayloadContent(
        syncId = syncId,
        typeSyncId = typeSyncId,
        timeStarted = timeStarted,
        timeEnded = timeEnded,
        comment = comment,
        tags = tags,
    )

    /**
     * Running timers sync as time entries with a null end time. A deletion
     * means another device stopped the timer (that device also pushed the
     * finished entry, which arrives through the regular entry path); the
     * local running record is then removed without creating a record.
     */
    private suspend fun applyServerRunningEntry(
        entry: TimeEntryDto,
        mappings: IdMappings,
    ) {
        val localTypeId = mappings.localIdOf(ENTITY_RUNNING_RECORD, entry.id)

        if (entry.deleted_at != null) {
            val targetTypeId = localTypeId ?: run {
                val actId = entry.activity_id.takeIf { it.isNotEmpty() }?.let { mappings.localIdOf(ENTITY_ACTIVITY, it) }
                if (actId != null && runningRecordRepo.has(actId)) {
                    val localRunning = runningRecordRepo.get(actId)
                    val serverStart = parseEpochMilli(entry.started_at)
                    if (serverStart == null || (localRunning != null && Math.abs(serverStart - localRunning.timeStarted) < 5000L)) {
                        actId
                    } else null
                } else null
            }
            if (targetTypeId != null) {
                if (runningRecordRepo.has(targetTypeId)) {
                    removeRunningRecordMediator.remove(typeId = targetTypeId)
                }
                mappings.forget(ENTITY_RUNNING_RECORD, entry.id)
                val existingSyncId = mappings.existingSyncId(ENTITY_RUNNING_RECORD, targetTypeId)
                if (existingSyncId != null) {
                    mappings.forget(ENTITY_RUNNING_RECORD, existingSyncId)
                    syncStateDao.remove(ENTITY_RUNNING_RECORD, existingSyncId)
                }
                syncStateDao.remove(ENTITY_RUNNING_RECORD, entry.id)
            }
            return
        }

        if (localTypeId != null) {
            if (runningRecordRepo.has(localTypeId)) {
                // Already running locally; the repository has no update for a
                // running start time, so keep the local value.
                return
            }
            // Mapped but not running locally: the local stop is newer than
            // the server state; the next push sends the tombstone.
            return
        }

        val typeLocalId = mappings.localIdOf(ENTITY_ACTIVITY, entry.activity_id)
            ?: run {
                Timber.w("Cannot import running record %s: unknown activity %s", entry.id, entry.activity_id)
                return
            }
        val timeStarted = parseEpochMilli(entry.started_at) ?: return
        val parsedTags = decodeTags(entry.tags)
        val localTags = localizeTags(parsedTags, mappings)
        Timber.i("Importing running record %s from sync", entry.id)
        // The timer may already run locally if a previous sync was
        // interrupted between starting it and writing the mapping; never
        // start it twice. Mapping and mirror are written after the start
        // so an interrupted import is retried idempotently.
        if (!runningRecordRepo.has(typeLocalId)) {
            addRunningRecordMediator.startTimer(
                typeId = typeLocalId,
                tags = localTags,
                comment = entry.comment,
                timeStarted = AddRunningRecordMediator.StartTime.Timestamp(timeStarted),
                checkDefaultDuration = false,
            )
        }
        mappings.remember(ENTITY_RUNNING_RECORD, typeLocalId, entry.id)
        syncStateDao.insertAll(
            listOf(
                SyncStateDBO(
                    entityType = ENTITY_RUNNING_RECORD,
                    entityId = entry.id,
                    contentHash = deltaCalculator.contentHash(
                        runningPayloadContent(
                            syncId = entry.id,
                            typeSyncId = entry.activity_id,
                            timeStarted = timeStarted,
                            comment = entry.comment,
                            tags = parsedTags,
                        ),
                    ),
                    syncedAt = System.currentTimeMillis(),
                ),
            ),
        )
    }

    private fun entryPayloadContent(
        syncId: String,
        typeSyncId: String,
        timeStarted: Long,
        timeEnded: Long,
        comment: String,
        tags: List<Map<String, Any?>>,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "activity_id" to typeSyncId,
        "started_at" to format(timeStarted),
        "ended_at" to if (timeEnded > 0) format(timeEnded) else null,
        "duration_seconds" to ((timeEnded - timeStarted) / 1000).toInt(),
        "comment" to comment,
        // Stored in the server's free text tags column as a json list.
        "tags" to encodeTags(tags),
    )

    private fun runningPayloadContent(
        syncId: String,
        typeSyncId: String,
        timeStarted: Long,
        comment: String,
        tags: List<Map<String, Any?>>,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "activity_id" to typeSyncId,
        "started_at" to format(timeStarted),
        "ended_at" to null,
        "duration_seconds" to 0,
        "comment" to comment,
        // Stored in the server's free text tags column as a json list.
        "tags" to encodeTags(tags),
    )

    private fun categoryPayloadContent(
        syncId: String,
        name: String,
        color: String,
        colorId: Int,
        note: String,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "name" to name,
        "color" to color,
        "color_id" to colorId,
        "note" to note,
    )

    private fun timetableEventPayloadContent(
        syncId: String,
        event: TimetableEvent,
        activitySyncId: String?,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "name" to event.name,
        "day_of_week" to event.dayOfWeek.toDouble(),
        "start_time" to event.startTime.toDouble(),
        "end_time" to event.endTime.toDouble(),
        "room" to event.room,
        "type" to event.type.ordinal.toDouble(),
        "comment" to event.comment,
        "activity_sync_id" to activitySyncId,
    )

    private fun timetableOverridePayloadContent(
        syncId: String,
        override: TimetableEventOverride,
        eventSyncId: String,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "date" to override.date,
        "event_sync_id" to eventSyncId,
        "room" to override.room,
        "start_time" to override.startTime.toDouble(),
        "end_time" to override.endTime.toDouble(),
        "cancelled" to override.cancelled,
        "note" to override.note,
    )

    private fun timetableDayPayloadContent(
        syncId: String,
        day: TimetableDay,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "date" to day.date,
        "free_day" to day.freeDay,
        "note" to day.note,
    )

    private fun timetableTodoPayloadContent(
        syncId: String,
        todo: TimetableTodo,
        eventSyncId: String,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "event_sync_id" to eventSyncId,
        "date" to todo.date,
        "text" to todo.text,
        "done" to todo.done,
        "type" to todo.type.ordinal.toDouble(),
    )

    private fun subjectGoalPayloadContent(
        syncId: String,
        goal: SubjectGoal,
        activitySyncId: String,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "activity_sync_id" to activitySyncId,
        "target_seconds" to goal.targetSeconds.toDouble(),
        "ects" to goal.ects,
    )

    private fun tagPayloadContent(
        syncId: String,
        name: String,
        icon: String,
        color: String,
        colorId: Int,
        iconColorSource: Long,
        note: String,
        archived: Boolean,
        valueType: String,
        valueSuffix: String,
    ): Map<String, Any?> = mapOf(
        "id" to syncId,
        "name" to name,
        "icon" to icon,
        "color" to color,
        "color_id" to colorId,
        "icon_color_source" to iconColorSource,
        "note" to note,
        "archived" to archived,
        "value_type" to valueType,
        "value_suffix" to valueSuffix,
    )

    /**
     * Tag list for payloads: one entry per tag with its sync id and the
     * optional numeric value, sorted by sync id for stable hashes.
     */
    private suspend fun tagsPayload(
        tags: List<RecordBase.Tag>,
        mappings: IdMappings,
    ): List<Map<String, Any?>> {
        return tags.mapNotNull { tag ->
            mappings.syncIdOf(ENTITY_RECORD_TAG, tag.tagId).let { syncId ->
                mapOf("id" to syncId, "value" to tag.numericValue)
            }
        }.sortedBy { it["id"] as String }
    }

    private fun localizeTags(
        tags: List<Map<String, Any?>>,
        mappings: IdMappings,
    ): List<RecordBase.Tag> {
        return tags.mapNotNull { tag ->
            val syncId = tag["id"] as? String ?: return@mapNotNull null
            val localId = mappings.localIdOf(ENTITY_RECORD_TAG, syncId) ?: return@mapNotNull null
            RecordBase.Tag(
                tagId = localId,
                numericValue = (tag["value"] as? Double),
            )
        }
    }

    private fun TimeEntryDto.toParsed(): ParsedEntry? {
        if (activity_id.isEmpty()) return null
        val timeStarted = parseEpochMilli(started_at) ?: return null
        val timeEnded = ended_at?.let { parseEpochMilli(it) ?: return null } ?: 0L
        return ParsedEntry(
            timeStarted = timeStarted,
            timeEnded = timeEnded,
            comment = comment,
            tags = decodeTags(tags),
        )
    }

    private fun encodeTags(tags: List<Map<String, Any?>>): String {
        return tagListAdapter.toJson(tags)
    }

    private fun decodeTags(json: String): List<Map<String, Any?>> {
        if (json.isEmpty()) return emptyList()
        return runCatching { tagListAdapter.fromJson(json) }.getOrNull() ?: emptyList()
    }

    private fun encodeStringList(values: List<String>): String {
        return stringListAdapter.toJson(values)
    }

    private fun decodeStringList(json: String): List<String> {
        if (json.isEmpty()) return emptyList()
        return runCatching { stringListAdapter.fromJson(json) }.getOrNull() ?: emptyList()
    }

    private fun parseEpochMilli(iso: String): Long? = runCatching {
        // The server sends timestamps with an explicit offset (+00:00)
        // while the app pushes Instant.toString with Z; accept both.
        OffsetDateTime.parse(iso).toInstant().toEpochMilli()
    }.recoverCatching {
        Instant.parse(iso).toEpochMilli()
    }.getOrNull()

    private suspend fun loadMappings(): IdMappings {
        val mappings = IdMappings()
        syncIdMapDao.getAll().forEach { row ->
            mappings.byLocal[row.entityType to row.localId] = row.syncId
            mappings.bySync[row.entityType to row.syncId] = row.localId
        }
        return mappings
    }

    private inner class IdMappings {
        val byLocal = mutableMapOf<Pair<String, Long>, String>()
        val bySync = mutableMapOf<Pair<String, String>, Long>()

        suspend fun syncIdOf(entityType: String, localId: Long): String {
            return existingSyncId(entityType, localId) ?: UUID.randomUUID().toString().also { syncId ->
                remember(entityType, localId, syncId)
            }
        }

        fun existingSyncId(entityType: String, localId: Long): String? = byLocal[entityType to localId]

        fun localIdOf(entityType: String, syncId: String): Long? = bySync[entityType to syncId]

        suspend fun remember(entityType: String, localId: Long, syncId: String) {
            syncIdMapDao.insert(
                SyncIdMapDBO(
                    entityType = entityType,
                    localId = localId,
                    syncId = syncId,
                ),
            )
            byLocal[entityType to localId] = syncId
            bySync[entityType to syncId] = localId
        }

        suspend fun forget(entityType: String, syncId: String) {
            syncIdMapDao.removeBySyncId(entityType, syncId)
            bySync.remove(entityType to syncId)?.let { localId ->
                byLocal.remove(entityType to localId)
            }
        }
    }

    private data class ParsedEntry(
        val timeStarted: Long,
        val timeEnded: Long,
        val comment: String,
        val tags: List<Map<String, Any?>> = emptyList(),
    )

    companion object {
        private const val PUSH_BATCH = 200
        private const val ENTITY_ACTIVITY = "activity"
        private const val ENTITY_TIME_ENTRY = "time_entry"
        private const val ENTITY_RUNNING_RECORD = "running_record"
        private const val ENTITY_CATEGORY = "category"
        private const val ENTITY_RECORD_TAG = "record_tag"
        private const val ENTITY_TIMETABLE_EVENT = "timetable_event"
        private const val ENTITY_TIMETABLE_OVERRIDE = "timetable_override"
        private const val ENTITY_TIMETABLE_DAY = "timetable_day"
        private const val ENTITY_TIMETABLE_TODO = "timetable_todo"
        private const val ENTITY_SUBJECT_GOAL = "subject_goal"
        private const val CONFLICT_UNKNOWN_TYPE = "unknown_type"
        private const val RESOLUTION_LOCAL_KEPT_NAME = "local_kept_name"
        private const val RESOLUTION_LOCAL_KEPT_START = "local_kept_start_time"
        private const val MIN_RECORD_DURATION_MILLIS = 60_000L

        private fun nowIso(): String = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        private fun format(epochMilli: Long): String =
            DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(epochMilli))
    }
}
