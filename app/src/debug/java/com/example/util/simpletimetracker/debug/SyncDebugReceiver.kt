package com.example.util.simpletimetracker.debug

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.net.Uri
import com.example.util.simpletimetracker.data_sync.db.SyncIdMapDao
import com.example.util.simpletimetracker.domain.record.interactor.AddRunningRecordMediator
import com.example.util.simpletimetracker.domain.record.model.RecordBase
import com.example.util.simpletimetracker.domain.record.repo.RecordRepo
import com.example.util.simpletimetracker.domain.record.repo.RunningRecordRepo
import com.example.util.simpletimetracker.domain.category.repo.CategoryRepo
import com.example.util.simpletimetracker.domain.category.repo.RecordTypeCategoryRepo
import com.example.util.simpletimetracker.domain.category.model.Category
import com.example.util.simpletimetracker.domain.color.model.AppColor
import com.example.util.simpletimetracker.domain.recordType.model.RecordType
import com.example.util.simpletimetracker.domain.record.model.Record
import com.example.util.simpletimetracker.domain.timetable.model.TimetableDay
import com.example.util.simpletimetracker.domain.timetable.model.TimetableEventOverride
import java.util.Calendar
import com.example.util.simpletimetracker.domain.recordType.repo.RecordTypeRepo
import com.example.util.simpletimetracker.domain.timetable.model.TimetableEvent
import com.example.util.simpletimetracker.domain.timetable.model.TimetableTodo
import com.example.util.simpletimetracker.domain.timetable.model.VacationPeriod
import com.example.util.simpletimetracker.data_sync.db.SyncStateDao
import com.example.util.simpletimetracker.data_sync.keystore.SyncCredentialStore
import com.example.util.simpletimetracker.data_sync.work.SyncScheduler
import dagger.hilt.android.AndroidEntryPoint
import javax.inject.Inject
import java.net.InetAddress
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import timber.log.Timber

/**
 * Debug only helper (shipped in the debug build type only): lets adb
 * trigger a sync or dump the sync state without touching the UI.
 *
 * Trigger a sync:
 *   adb shell am broadcast -a de.piusdischinger.timetracker.debug.SYNC_NOW \
 *     -n de.piusdischinger.timetracker.dev/com.example.util.simpletimetracker.debug.SyncDebugReceiver
 *
 * Dump sync state:
 *   adb shell am broadcast -a de.piusdischinger.timetracker.debug.DUMP_STATE \
 *     -n de.piusdischinger.timetracker.dev/com.example.util.simpletimetracker.debug.SyncDebugReceiver
 */
@AndroidEntryPoint
class SyncDebugReceiver : BroadcastReceiver() {

    @Inject
    lateinit var syncScheduler: SyncScheduler

    @Inject
    lateinit var syncEngine: com.example.util.simpletimetracker.data_sync.engine.SyncEngine

    @Inject
    lateinit var recordRepo: RecordRepo

    @Inject
    lateinit var recordInteractor: com.example.util.simpletimetracker.domain.record.interactor.RecordInteractor

    @Inject
    lateinit var runningRecordRepo: RunningRecordRepo

    @Inject
    lateinit var recordTypeRepo: RecordTypeRepo

    @Inject
    lateinit var syncStateDao: SyncStateDao

    @Inject
    lateinit var syncIdMapDao: SyncIdMapDao

    @Inject
    lateinit var credentialStore: SyncCredentialStore

    @Inject
    lateinit var categoryRepo: CategoryRepo

    @Inject
    lateinit var recordTypeCategoryRepo: RecordTypeCategoryRepo

    @Inject
    lateinit var addRunningRecordMediator: AddRunningRecordMediator

    @Inject
    lateinit var removeRunningRecordMediator: com.example.util.simpletimetracker.domain.record.interactor.RemoveRunningRecordMediator

    @Inject
    lateinit var timetableRepo: com.example.util.simpletimetracker.domain.timetable.repo.TimetableRepo

    @Inject
    lateinit var prefsInteractor: com.example.util.simpletimetracker.domain.prefs.interactor.PrefsInteractor

    @Inject
    lateinit var icsImportInteractor: com.example.util.simpletimetracker.domain.timetable.ics.IcsImportInteractor

    @Inject
    lateinit var subjectGoalRepo: com.example.util.simpletimetracker.domain.timetable.repo.SubjectGoalRepo

    @Inject
    lateinit var timetableNotificationInteractor: com.example.util.simpletimetracker.domain.timetable.notification.TimetableNotificationInteractor

    @Inject
    lateinit var recordRepeatInteractor: com.example.util.simpletimetracker.core.interactor.RecordRepeatInteractor

    @Inject
    lateinit var recordTypeInteractor: com.example.util.simpletimetracker.domain.recordType.interactor.RecordTypeInteractor

    override fun onReceive(context: Context, intent: Intent) {
        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                when (intent.action) {
                    ACTION_SYNC_NOW -> {
                        Timber.i("DebugReceiver: direct sync requested")
                        syncEngine.syncNow()
                    }
                    ACTION_DUMP_STATE -> dumpState()
                    ACTION_DNS_TEST -> {
                        val host = intent.getStringExtra(EXTRA_HOST)
                            ?: Uri.parse(credentialStore.serverUrl).host.orEmpty()
                        if (host.isNotEmpty()) {
                            Timber.i("DebugReceiver: dns test for %s", host)
                            try {
                                val addresses = InetAddress.getAllByName(host)
                                Timber.i("DebugReceiver: dns ok: %s", addresses.joinToString())
                            } catch (e: Exception) {
                                Timber.e(e, "DebugReceiver: dns FAILED for %s", host)
                            }
                            // Raw connect to the tailnet ip bypassing dns.
                            try {
                                java.net.Socket().use { socket ->
                                    socket.connect(java.net.InetSocketAddress("100.64.0.9", 443), 3000)
                                    Timber.i("DebugReceiver: raw connect to 100.64.0.9:443 ok")
                                }
                            } catch (e: Exception) {
                                Timber.e(e, "DebugReceiver: raw connect to 100.64.0.9:443 FAILED")
                            }
                        }
                    }
                    ACTION_SET_CREDENTIALS -> {
                        val url = intent.getStringExtra(EXTRA_URL).orEmpty()
                        val username = intent.getStringExtra(EXTRA_USERNAME).orEmpty()
                        val token = intent.getStringExtra(EXTRA_TOKEN).orEmpty()
                        if (url.isNotEmpty() && username.isNotEmpty() && token.isNotEmpty()) {
                            credentialStore.serverUrl = url
                            credentialStore.username = username
                            credentialStore.apiToken = token
                            Timber.i("DebugReceiver: credentials stored for %s", username)
                            syncScheduler.syncNow()
                        } else {
                            Timber.w("DebugReceiver: set credentials called with missing extras")
                        }
                    }
                    ACTION_WIPE_LOCAL -> {
                        Timber.i("DebugReceiver: wiping local data for a clean first sync")
                        recordRepo.clear()
                        runningRecordRepo.clear()
                        recordTypeRepo.clear()
                        syncStateDao.clear()
                        syncIdMapDao.clear()
                        Timber.i("DebugReceiver: local data wiped")
                    }
                    ACTION_ADD_UNI_TEST_DATA -> addUniTestData()
                    ACTION_START_TIMER -> startTimer(intent.getStringExtra(EXTRA_NAME).orEmpty())
                    ACTION_STOP_TIMER -> {
                        val name = intent.getStringExtra(EXTRA_NAME)
                        val save = intent.getBooleanExtra(EXTRA_SAVE_RECORD, true)
                        stopTimer(name, save)
                    }
                    ACTION_STOP_ALL_TIMERS -> stopAllTimers()
                    ACTION_ADD_ACTIVITY -> {
                        val name = intent.getStringExtra(EXTRA_NAME).orEmpty()
                        val colorId = intent.getIntExtra(EXTRA_COLOR_ID, 1)
                        val icon = intent.getStringExtra(EXTRA_ICON) ?: "ic_play_arrow_24px"
                        if (name.isNotEmpty()) addActivity(name, colorId, icon)
                    }
                    ACTION_RENAME_ACTIVITY -> {
                        val oldName = intent.getStringExtra(EXTRA_NAME).orEmpty()
                        val newName = intent.getStringExtra(EXTRA_NEW_NAME).orEmpty()
                        if (oldName.isNotEmpty() && newName.isNotEmpty()) renameActivity(oldName, newName)
                    }
                    ACTION_DELETE_RECORD -> {
                        val name = intent.getStringExtra(EXTRA_NAME)
                        deleteLatestRecord(name)
                    }
                    ACTION_ADD_RECORD -> {
                        val name = intent.getStringExtra(EXTRA_NAME).orEmpty()
                        val durSec = intent.getLongExtra("durSec", 0L)
                        val now = System.currentTimeMillis()
                        val start = now - durSec * 1000L
                        val type = recordTypeRepo.get(name).firstOrNull()
                        if (type != null) {
                            recordInteractor.add(Record(id = 0, typeId = type.id, timeStarted = start, timeEnded = now, comment = "test", tags = emptyList()))
                            Timber.i("DebugReceiver: add record for %s with durSec=%d", name, durSec)
                        }
                    }
                    ACTION_DELETE_ACTIVITY -> {
                        val name = intent.getStringExtra(EXTRA_NAME).orEmpty()
                        if (name.isNotEmpty()) deleteActivity(name)
                    }
                    ACTION_DUMP_ACTIVITIES -> dumpActivities()
                    ACTION_QS_TILE_REPEAT -> qsTileRepeat()
                    ACTION_DUMP_RUNNING -> dumpRunning()
                    ACTION_DUMP_RECORDS -> dumpRecords()
                    ACTION_SEED_TIMETABLE -> seedTimetable()
                    ACTION_IMPORT_ICS -> importIcs(intent.getStringExtra(EXTRA_PATH).orEmpty())
                    ACTION_DUMP_TIMETABLE -> dumpTimetable()
                    ACTION_RESCHEDULE_TIMETABLE -> rescheduleTimetable()
                    ACTION_RESET_TIMETABLE_MIRROR -> resetTimetableMirror()
                    ACTION_TIMETABLE_TODO_DONE -> {
                        val eventId = intent.getLongExtra(EXTRA_EVENT_ID, 0L)
                        val date = intent.getStringExtra(EXTRA_DATE).orEmpty()
                        val type = com.example.util.simpletimetracker.domain.timetable.model.TimetableTodo.Type
                            .entries.getOrNull(intent.getIntExtra(EXTRA_TODO_TYPE, 0))
                        if (eventId != 0L && date.isNotEmpty() && type != null) {
                            timetableNotificationInteractor.onTodoDone(eventId, date, type)
                            Timber.i("DebugReceiver: todo done %d %s %s", eventId, date, type)
                        }
                    }
                    ACTION_SET_TODO_DONE -> {
                        val todoId = intent.getLongExtra(EXTRA_TODO_ID, 0L)
                        val done = intent.getBooleanExtra(EXTRA_DONE, true)
                        if (todoId != 0L) {
                            timetableRepo.setTodoDone(todoId, done)
                            Timber.i("DebugReceiver: set todo id=%d done=%b", todoId, done)
                        }
                    }
                    ACTION_WIPE_ALL -> wipeAll()
                    ACTION_RESET_CLEAN_DATASET -> resetCleanDataset()
                    ACTION_PRUNE_SHORT_RECORDS -> pruneShortRecords()
                    ACTION_SET_PREF_LONG -> {
                        val key = intent.getStringExtra(EXTRA_KEY).orEmpty()
                        val value = intent.getLongExtra(EXTRA_VALUE, 0L)
                        when (key) {
                            "endOfDayShift" -> prefsInteractor.setEndOfDayShift(value)
                            "timetablePrepLead" -> prefsInteractor.setTimetablePrepLead(value)
                            "daysInCalendar" -> {
                                val days = com.example.util.simpletimetracker.domain.daysOfWeek.model.DaysInCalendar.entries
                                    .getOrNull(value.toInt())
                                if (days != null) prefsInteractor.setDaysInCalendar(days)
                            }
                            else -> Timber.w("DebugReceiver: unknown pref %s", key)
                        }
                        Timber.i("DebugReceiver: set %s=%d", key, value)
                    }
                    ACTION_SET_VACATION_PERIOD -> {
                        val start = intent.getStringExtra(EXTRA_START).orEmpty()
                        val end = intent.getStringExtra(EXTRA_END).orEmpty()
                        if (start.isNotEmpty() && end.isNotEmpty()) {
                            val period = VacationPeriod(
                                start = java.time.LocalDate.parse(start),
                                end = java.time.LocalDate.parse(end),
                            )
                            prefsInteractor.setVacationPeriods(listOf(period))
                        } else {
                            prefsInteractor.setVacationPeriods(emptyList())
                        }
                        timetableNotificationInteractor.rescheduleAll()
                        Timber.i("DebugReceiver: vacation period %s..%s", start, end)
                    }
                    ACTION_SET_PREF_BOOL -> {
                        val key = intent.getStringExtra(EXTRA_KEY).orEmpty()
                        val value = intent.getBooleanExtra(EXTRA_VALUE, false)
                        when (key) {
                            "enablePomodoroMode" -> prefsInteractor.setEnablePomodoroMode(value)
                            "allowMultitasking" -> prefsInteractor.setAllowMultitasking(value)
                            "enableRepeatButton" -> prefsInteractor.setEnableRepeatButton(value)
                            "reverseOrderInCalendar" -> prefsInteractor.setReverseOrderInCalendar(value)
                            "showGoalsSeparately" -> prefsInteractor.setShowGoalsSeparately(value)
                            "showRecordsCalendar" -> prefsInteractor.setShowRecordsCalendar(value)
                            "showUniTab" -> prefsInteractor.setShowUniTab(value)
                            "vacationMode" -> prefsInteractor.setVacationMode(value)
                            else -> Timber.w("DebugReceiver: unknown pref %s", key)
                        }
                        Timber.i("DebugReceiver: set %s=%b", key, value)
                    }
                    else -> Timber.w("DebugReceiver: unknown action %s", intent.action)
                }
            } finally {
                pending.finish()
            }
        }
    }

    private suspend fun wipeAll() {
        recordRepo.clear()
        runningRecordRepo.clear()
        recordTypeRepo.clear()
        categoryRepo.clear()
        recordTypeCategoryRepo.clear()
        timetableRepo.clearAll()
        subjectGoalRepo.clear()
        syncIdMapDao.clear()
        syncStateDao.clear()
        // Reset the pull marker so the next sync imports the full
        // server state instead of nothing.
        credentialStore.lastSyncMarker = ""
        Timber.i("DebugReceiver: all local data wiped")
    }

    /**
     * Timestamp helper for the fictional test dataset.
     */
    private fun timestamp(year: Int, month: Int, day: Int, hour: Int, minute: Int): Long {
        val calendar = Calendar.getInstance()
        calendar.clear()
        calendar.set(year, month - 1, day, hour, minute, 0)
        return calendar.timeInMillis
    }

    private suspend fun seedTimetable() {
        val uniCategory = categoryRepo.get("Uni").firstOrNull()
        val uniCategoryId = uniCategory?.id
            ?: categoryRepo.add(Category(id = 0, name = "Uni", color = AppColor(colorId = 10, colorInt = ""), note = ""))

        // One tile per subject in the green palette range, with a text
        // icon (no emoji) instead of the unknown icon placeholder.
        val seedActivities = listOf(
            Triple("Thermodynamik", "Thermo", 9), // teal
            Triple("Analysis", "Analysis", 10), // green
            Triple("Java", "Java", 11), // light green
            Triple("TheoInfo", "TheoInfo", 12), // lime
        )
        // Everyday activities outside the Uni category, with real icons.
        val everydayActivities = listOf(
            Triple("Pause", "ic_free_breakfast_24px", 12),
            Triple("Essen", "ic_restaurant_24px", 9),
            Triple("Schlafen", "ic_single_bed_24px", 1),
            Triple("Sport", "ic_fitness_center_24px", 4),
            Triple("Lesen", "ic_menu_book_24px", 5),
        )
        val activityIds = mutableMapOf<String, Long>()
        (seedActivities).forEach { (name, iconText, colorId) ->
            val existing = recordTypeRepo.get(name).firstOrNull()
            val id = existing?.id ?: recordTypeRepo.add(
                RecordType(
                    id = 0,
                    name = name,
                    icon = iconText,
                    color = AppColor(colorId = colorId, colorInt = ""),
                    defaultDuration = 0,
                    note = "",
                ),
            )
            activityIds[name] = id
            if (uniCategoryId != null) {
                recordTypeCategoryRepo.addTypes(uniCategoryId, listOf(id))
            }
        }
        everydayActivities.forEach { (name, iconText, colorId) ->
            val existing = recordTypeRepo.get(name).firstOrNull()
            if (existing == null) {
                recordTypeRepo.add(
                    RecordType(
                        id = 0,
                        name = name,
                        icon = iconText,
                        color = AppColor(colorId = colorId, colorInt = ""),
                        defaultDuration = 0,
                        note = "",
                    ),
                )
            }
        }

        timetableRepo.clearEvents()

        // Fictional timetable per subject; the event type distinguishes
        // lecture, exercise and tutorium slots of the same subject.
        val events = listOf(
            // Monday
            TimetableEvent(name = "Thermo", dayOfWeek = 1, startTime = 8 * 60 + 15, endTime = 9 * 60 + 45, room = "HS 1", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["Thermodynamik"]),
            TimetableEvent(name = "Analysis", dayOfWeek = 1, startTime = 10 * 60 + 15, endTime = 11 * 60 + 45, room = "HS 2", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["Analysis"]),
            TimetableEvent(name = "Java", dayOfWeek = 1, startTime = 14 * 60 + 15, endTime = 15 * 60 + 45, room = "HS 3", type = TimetableEvent.Type.LECTURE, comment = "Laptop mitbringen", activityTypeId = activityIds["Java"]),
            // Tuesday
            TimetableEvent(name = "Java", dayOfWeek = 2, startTime = 8 * 60 + 15, endTime = 9 * 60 + 45, room = "HS 3", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["Java"]),
            TimetableEvent(name = "Thermo", dayOfWeek = 2, startTime = 12 * 60, endTime = 13 * 60 + 30, room = "R 2.104", type = TimetableEvent.Type.EXERCISE, comment = "", activityTypeId = activityIds["Thermodynamik"]),
            // Wednesday
            TimetableEvent(name = "Thermo", dayOfWeek = 3, startTime = 10 * 60 + 15, endTime = 11 * 60 + 45, room = "HS 1", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["Thermodynamik"]),
            TimetableEvent(name = "TheoInfo", dayOfWeek = 3, startTime = 14 * 60 + 15, endTime = 15 * 60 + 45, room = "HS 4", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["TheoInfo"]),
            // Thursday
            TimetableEvent(name = "Analysis", dayOfWeek = 4, startTime = 10 * 60 + 15, endTime = 11 * 60 + 45, room = "HS 2", type = TimetableEvent.Type.LECTURE, comment = "", activityTypeId = activityIds["Analysis"]),
            TimetableEvent(name = "Java", dayOfWeek = 4, startTime = 12 * 60, endTime = 13 * 60 + 30, room = "R 0.014", type = TimetableEvent.Type.EXERCISE, comment = "", activityTypeId = activityIds["Java"]),
            TimetableEvent(name = "TheoInfo", dayOfWeek = 4, startTime = 16 * 60, endTime = 17 * 60 + 30, room = "R 0.201", type = TimetableEvent.Type.TUTORIUM, comment = "", activityTypeId = activityIds["TheoInfo"]),
            // Friday
            TimetableEvent(name = "Analysis", dayOfWeek = 5, startTime = 10 * 60 + 15, endTime = 11 * 60 + 45, room = "R 1.012", type = TimetableEvent.Type.EXERCISE, comment = "", activityTypeId = activityIds["Analysis"]),
            TimetableEvent(name = "TheoInfo", dayOfWeek = 5, startTime = 12 * 60, endTime = 13 * 60 + 30, room = "R 0.207", type = TimetableEvent.Type.EXERCISE, comment = "", activityTypeId = activityIds["TheoInfo"]),
        )

        val eventIds = mutableMapOf<TimetableEvent, Long>()
        events.forEach { event ->
            val id = timetableRepo.addEvent(event)
            eventIds[event] = id
            if (event.type == TimetableEvent.Type.LECTURE) {
                timetableRepo.addTodo(
                    TimetableTodo(eventId = id, date = null, text = "Vorbereitung: Skript lesen", done = false, type = TimetableTodo.Type.PREPARATION),
                )
                timetableRepo.addTodo(
                    TimetableTodo(eventId = id, date = null, text = "Nachbereitung: Zusammenfassung", done = false, type = TimetableTodo.Type.FOLLOW_UP),
                )
            } else {
                timetableRepo.addTodo(
                    TimetableTodo(eventId = id, date = null, text = "Vorbereitung: Aufgabenblatt", done = false, type = TimetableTodo.Type.PREPARATION),
                )
            }
        }
        // Vorlesungsfreier Tag: naechster Mittwoch (Dies Academicus).
        timetableRepo.addDay(
            TimetableDay(id = 0, date = "2026-10-07", freeDay = true, note = "Dies Academicus"),
        )

        // Raumaenderung: Montag Thermo VL in HS 2 statt HS 1.
        val mondayThermoId = eventIds.filterKeys { it.dayOfWeek == 1 && it.name == "Thermo" }.values.first()
        timetableRepo.addOverride(
            TimetableEventOverride(
                id = 0,
                date = "2026-10-05",
                eventId = mondayThermoId,
                room = "HS 2",
                startTime = 8 * 60 + 15,
                endTime = 9 * 60 + 45,
                cancelled = false,
                note = "Raumaenderung",
            ),
        )

        // Attendance records of the past week for verification:
        // attended slots get a record, missed slots stay without one.
        val attendanceRecords = listOf(
            // Monday 2026-09-28: both lectures attended.
            Triple("Thermodynamik", timestamp(2026, 9, 28, 8, 20), timestamp(2026, 9, 28, 9, 40)),
            Triple("Analysis", timestamp(2026, 9, 28, 10, 20), timestamp(2026, 9, 28, 11, 40)),
            // Tuesday 2026-09-29: Thermo exercise missed, Java lecture attended.
            Triple("Java", timestamp(2026, 9, 29, 8, 20), timestamp(2026, 9, 29, 9, 40)),
            // Wednesday 2026-09-30: both attended.
            Triple("Thermodynamik", timestamp(2026, 9, 30, 10, 20), timestamp(2026, 9, 30, 11, 40)),
            Triple("TheoInfo", timestamp(2026, 9, 30, 14, 20), timestamp(2026, 9, 30, 15, 40)),
            // Thursday 2026-10-01: Analysis attended, Java exercise attended.
            Triple("Analysis", timestamp(2026, 10, 1, 10, 20), timestamp(2026, 10, 1, 11, 40)),
            Triple("Java", timestamp(2026, 10, 1, 12, 5), timestamp(2026, 10, 1, 13, 25)),
            Triple("Lesen", timestamp(2026, 10, 1, 20, 0), timestamp(2026, 10, 1, 21, 0)),
            // Friday 2026-10-02: Analysis exercise missed, TheoInfo exercise attended.
            Triple("TheoInfo", timestamp(2026, 10, 2, 12, 5), timestamp(2026, 10, 2, 13, 25)),
            Triple("Sport", timestamp(2026, 10, 2, 17, 0), timestamp(2026, 10, 2, 18, 30)),
        )
        attendanceRecords.forEach { (name, start, end) ->
            val typeId = recordTypeRepo.get(name).firstOrNull()?.id ?: return@forEach
            recordRepo.add(Record(id = 0, typeId = typeId, timeStarted = start, timeEnded = end, comment = "", tags = emptyList()))
        }

        Timber.i(
            "DebugReceiver: seeded %d events, %d activities, %d attendance records",
            events.size,
            seedActivities.size,
            attendanceRecords.size,
        )
    }

    private suspend fun startTimer(name: String) {
        val type = recordTypeRepo.get(name).firstOrNull()
        if (type == null) {
            Timber.w("DebugReceiver: unknown activity %s", name)
            return
        }
        addRunningRecordMediator.startTimer(
            typeId = type.id,
            tags = emptyList<RecordBase.Tag>(),
            comment = "",
            timeStarted = AddRunningRecordMediator.StartTime.TakeCurrent,
        )
        Timber.i("DebugReceiver: started %s (id=%d)", name, type.id)
    }

    private suspend fun resetTimetableMirror() {
        val timetableTypes = listOf(
            "timetable_event",
            "timetable_override",
            "timetable_day",
            "timetable_todo",
            "subject_goal",
        )
        syncStateDao.getAll()
            .filter { it.entityType in timetableTypes }
            .forEach { syncStateDao.remove(it.entityType, it.entityId) }
        Timber.i("DebugReceiver: timetable mirror reset, next sync re-pushes")
    }

    private suspend fun rescheduleTimetable() {
        timetableNotificationInteractor.rescheduleAll()
        Timber.i("DebugReceiver: timetable notifications rescheduled")
    }

    private suspend fun importIcs(path: String) {
        if (path.isEmpty()) {
            Timber.w("DebugReceiver: import ics called without path")
            return
        }
        val uri = android.net.Uri.fromFile(java.io.File(path)).toString()
        val result = icsImportInteractor.importFile(uri)
        Timber.i(
            "DebugReceiver: ics import added %d events, unmatched: %s",
            result.eventsAdded,
            result.unmatchedNames.joinToString(),
        )
    }

    private suspend fun dumpTimetable() {
        val events = timetableRepo.getAllEvents()
        val overrides = timetableRepo.getAllOverrides()
        val todos = timetableRepo.getAllTodos()
        Timber.i("DebugReceiver: %d timetable events, %d overrides, %d todos", events.size, overrides.size, todos.size)
        events.forEach {
            Timber.i("  Event: id=%d %s day=%d %d..%d room=%s typeId=%s", it.id, it.name, it.dayOfWeek, it.startTime, it.endTime, it.room, it.activityTypeId)
        }
        overrides.forEach {
            Timber.i("  Override: id=%d date=%s eventId=%d room=%s %d..%d cancelled=%b", it.id, it.date, it.eventId, it.room, it.startTime, it.endTime, it.cancelled)
        }
        todos.forEach {
            Timber.i("  Todo: id=%d eventId=%d date=%s text='%s' done=%b type=%s", it.id, it.eventId, it.date, it.text, it.done, it.type)
        }
    }

    private suspend fun stopAllTimers() {
        val running = runningRecordRepo.getAll()
        running.forEach { runningRecordRepo.remove(it.id) }
        Timber.i("DebugReceiver: stopped %d timers (without records)", running.size)
    }

    private suspend fun stopTimer(name: String?, saveRecord: Boolean) {
        val allRunning = runningRecordRepo.getAll()
        val running = if (name.isNullOrEmpty()) {
            allRunning
        } else {
            val type = recordTypeRepo.get(name).firstOrNull()
            if (type != null) allRunning.filter { it.id == type.id } else emptyList()
        }
        running.forEach { r ->
            if (saveRecord) {
                removeRunningRecordMediator.removeWithRecordAdd(r)
            } else {
                runningRecordRepo.remove(r.id)
            }
        }
        Timber.i("DebugReceiver: stopped %d timers (saveRecord=%b)", running.size, saveRecord)
    }

    private suspend fun addActivity(name: String, colorId: Int, icon: String) {
        val existing = recordTypeRepo.get(name).firstOrNull()
        if (existing == null) {
            val id = recordTypeRepo.add(
                RecordType(
                    id = 0,
                    name = name,
                    icon = icon,
                    color = AppColor(colorId = colorId, colorInt = ""),
                    defaultDuration = 0,
                    note = "",
                ),
            )
            Timber.i("DebugReceiver: created activity %s (id=%d)", name, id)
        } else {
            Timber.i("DebugReceiver: activity %s already exists (id=%d)", name, existing.id)
        }
    }

    private suspend fun renameActivity(oldName: String, newName: String) {
        val existing = recordTypeRepo.get(oldName).firstOrNull()
        if (existing != null) {
            recordTypeRepo.add(existing.copy(name = newName))
            Timber.i("DebugReceiver: renamed activity %s -> %s (id=%d)", oldName, newName, existing.id)
        } else {
            Timber.w("DebugReceiver: activity %s not found to rename", oldName)
        }
    }

    private suspend fun deleteActivity(name: String) {
        val existing = recordTypeRepo.get(name).firstOrNull()
        if (existing != null) {
            recordTypeInteractor.remove(existing.id)
            Timber.i("DebugReceiver: deleted activity %s (id=%d)", name, existing.id)
        } else {
            Timber.w("DebugReceiver: activity %s not found to delete", name)
        }
    }

    private suspend fun deleteLatestRecord(name: String?) {
        val records = recordRepo.getAll().sortedByDescending { it.timeEnded }
        val target = if (name.isNullOrEmpty()) {
            records.firstOrNull()
        } else {
            val type = recordTypeRepo.get(name).firstOrNull()
            if (type != null) records.firstOrNull { it.typeId == type.id } else null
        }
        if (target != null) {
            recordRepo.remove(target.id)
            Timber.i("DebugReceiver: deleted record id=%d", target.id)
        } else {
            Timber.w("DebugReceiver: no record found to delete")
        }
    }

    private suspend fun dumpActivities() {
        val types = recordTypeRepo.getAll()
        Timber.i("DebugReceiver: %d activities:", types.size)
        types.forEach {
            Timber.i("  activity id=%d name='%s' color=%d icon='%s'", it.id, it.name, it.color.colorId, it.icon)
        }
    }

    // Executes the quick settings tile logic for testing.
    private suspend fun qsTileRepeat() {
        recordRepeatInteractor.repeatForQuickTileExternal()
        Timber.i("DebugReceiver: qs tile repeat executed")
    }

    private suspend fun dumpRunning() {
        val running = runningRecordRepo.getAll()
        val now = System.currentTimeMillis()
        Timber.i("DebugReceiver: running=%d", running.size)
        running.forEach { r ->
            val name = recordTypeRepo.get(r.id)?.name ?: "?"
            val durationMin = (now - r.timeStarted) / 60000
            Timber.i("  running '%s' started=%d (%d min ago)", name, r.timeStarted, durationMin)
        }
    }

    private suspend fun pruneShortRecords() {
        val short = recordRepo.getAll().filter { it.timeEnded > 0 && (it.timeEnded - it.timeStarted) < 60_000L }
        short.forEach { r ->
            recordRepo.remove(r.id)
            Timber.i("DebugReceiver: pruned short record id=%d (duration=%d ms)", r.id, r.timeEnded - r.timeStarted)
        }
        Timber.i("DebugReceiver: pruned %d short records", short.size)
    }

    private suspend fun dumpRecords() {
        val records = recordRepo.getAll().sortedByDescending { it.timeEnded }.take(10)
        Timber.i("DebugReceiver: latest %d records:", records.size)
        records.forEach { r ->
            val name = recordTypeRepo.get(r.typeId)?.name ?: "?"
            val durSec = (r.timeEnded - r.timeStarted) / 1000
            Timber.i("  record id=%d '%s' start=%d end=%d (%d s / %d min)", r.id, name, r.timeStarted, r.timeEnded, durSec, durSec / 60)
        }
    }

    private suspend fun addUniTestData() {
        val existing = categoryRepo.get("Uni").firstOrNull()
        val categoryId = existing?.id ?: categoryRepo.add(
            Category(
                id = 0,
                name = "Uni",
                color = AppColor(colorId = 5, colorInt = ""),
                note = "",
            ),
        )
        val assigned = recordTypeCategoryRepo.getTypeIdsByCategory(categoryId)
        recordTypeRepo.getAll()
            .filter { it.name in listOf("Vorlesung", "Lernen") && it.id !in assigned }
            .forEach { recordTypeCategoryRepo.addTypes(categoryId, listOf(it.id)) }
        val typeIds = recordTypeCategoryRepo.getTypeIdsByCategory(categoryId)
        Timber.i("DebugReceiver: uni category id=%d types=%s", categoryId, typeIds)
    }

    private suspend fun resetCleanDataset() {
        Timber.i("DebugReceiver: starting resetCleanDataset...")
        // 1. Stop any running timers
        val running = runningRecordRepo.getAll()
        running.forEach { runningRecordRepo.remove(it.id) }
        Timber.i("DebugReceiver: stopped %d running timers", running.size)

        // 2. Remove all records locally so tombstones are generated
        val allRecords = recordRepo.getAll()
        allRecords.forEach { recordRepo.remove(it.id) }
        Timber.i("DebugReceiver: removed %d local records", allRecords.size)

        // 3. Clear timetable and subject goals
        timetableRepo.clearAll()
        subjectGoalRepo.clear()
        Timber.i("DebugReceiver: cleared timetable and subject goals")

        // 4. Sync to push tombstones for old records and timetable to server
        runCatching { syncEngine.syncNow() }
            .onSuccess { Timber.i("DebugReceiver: tombstones pushed to server") }
            .onFailure { Timber.e(it, "DebugReceiver: failed to push tombstones") }

        // 5. Seed the clean timetable, activities, category, and 10 attendance records
        seedTimetable()

        // 6. Set clean subject goals (75h / 2.5 ECTS for the 4 Uni subjects)
        val uniSubjects = listOf("Thermodynamik", "Analysis", "Java", "TheoInfo")
        uniSubjects.forEach { name ->
            val type = recordTypeRepo.get(name).firstOrNull()
            if (type != null) {
                subjectGoalRepo.set(
                    com.example.util.simpletimetracker.domain.timetable.model.SubjectGoal(
                        activityTypeId = type.id,
                        targetSeconds = 75L * 3600L,
                        ects = 2.5,
                    ),
                )
            }
        }
        Timber.i("DebugReceiver: configured subject goals for uni subjects")

        // 7. Sync new clean dataset to server
        runCatching { syncEngine.syncNow() }
            .onSuccess { Timber.i("DebugReceiver: clean dataset synced successfully to server") }
            .onFailure { Timber.e(it, "DebugReceiver: failed to sync clean dataset") }
    }

    private suspend fun dumpState() {
        val mirror = syncStateDao.getAll()
        val mappings = syncIdMapDao.getAll()
        Timber.i(
            "DebugReceiver state: url=%s user=%s token=%s marker=%s mirrorEntries=%d idMappings=%d",
            credentialStore.serverUrl,
            credentialStore.username,
            credentialStore.apiToken,
            credentialStore.lastSyncMarker,
            mirror.size,
            mappings.size,
        )
        mirror.take(50).forEach {
            Timber.i("  mirror %s %s hash=%s", it.entityType, it.entityId, it.contentHash.take(10))
        }
    }

    companion object {
        const val ACTION_SYNC_NOW = "de.piusdischinger.timetracker.debug.SYNC_NOW"
        const val ACTION_DUMP_STATE = "de.piusdischinger.timetracker.debug.DUMP_STATE"
        const val ACTION_SET_CREDENTIALS = "de.piusdischinger.timetracker.debug.SET_CREDENTIALS"
        const val ACTION_DNS_TEST = "de.piusdischinger.timetracker.debug.DNS_TEST"
        const val ACTION_WIPE_LOCAL = "de.piusdischinger.timetracker.debug.WIPE_LOCAL"
        const val ACTION_ADD_UNI_TEST_DATA = "de.piusdischinger.timetracker.debug.ADD_UNI_TEST_DATA"
        const val ACTION_START_TIMER = "de.piusdischinger.timetracker.debug.START_TIMER"
        const val ACTION_STOP_TIMER = "de.piusdischinger.timetracker.debug.STOP_TIMER"
        const val ACTION_STOP_ALL_TIMERS = "de.piusdischinger.timetracker.debug.STOP_ALL_TIMERS"
        const val ACTION_ADD_ACTIVITY = "de.piusdischinger.timetracker.debug.ADD_ACTIVITY"
        const val ACTION_RENAME_ACTIVITY = "de.piusdischinger.timetracker.debug.RENAME_ACTIVITY"
        const val ACTION_DELETE_ACTIVITY = "de.piusdischinger.timetracker.debug.DELETE_ACTIVITY"
        const val ACTION_DELETE_RECORD = "de.piusdischinger.timetracker.debug.DELETE_RECORD"
        const val ACTION_ADD_RECORD = "de.piusdischinger.timetracker.debug.ADD_RECORD"
        const val ACTION_RESET_CLEAN_DATASET = "de.piusdischinger.timetracker.debug.RESET_CLEAN_DATASET"
        const val ACTION_DUMP_ACTIVITIES = "de.piusdischinger.timetracker.debug.DUMP_ACTIVITIES"
        const val ACTION_QS_TILE_REPEAT = "de.piusdischinger.timetracker.debug.QS_TILE_REPEAT"
        const val ACTION_DUMP_RUNNING = "de.piusdischinger.timetracker.debug.DUMP_RUNNING"
        const val ACTION_DUMP_RECORDS = "de.piusdischinger.timetracker.debug.DUMP_RECORDS"
        const val ACTION_SEED_TIMETABLE = "de.piusdischinger.timetracker.debug.SEED_TIMETABLE"
        const val ACTION_IMPORT_ICS = "de.piusdischinger.timetracker.debug.IMPORT_ICS"
        const val ACTION_DUMP_TIMETABLE = "de.piusdischinger.timetracker.debug.DUMP_TIMETABLE"
        const val ACTION_RESCHEDULE_TIMETABLE = "de.piusdischinger.timetracker.debug.RESCHEDULE_TIMETABLE"
        const val ACTION_RESET_TIMETABLE_MIRROR = "de.piusdischinger.timetracker.debug.RESET_TIMETABLE_MIRROR"
        const val ACTION_TIMETABLE_TODO_DONE = "de.piusdischinger.timetracker.debug.TIMETABLE_TODO_DONE"
        const val ACTION_SET_TODO_DONE = "de.piusdischinger.timetracker.debug.SET_TODO_DONE"
        const val ACTION_WIPE_ALL = "de.piusdischinger.timetracker.debug.WIPE_ALL"
        const val ACTION_PRUNE_SHORT_RECORDS = "de.piusdischinger.timetracker.debug.PRUNE_SHORT_RECORDS"
        const val ACTION_SET_PREF_BOOL = "de.piusdischinger.timetracker.debug.SET_PREF_BOOL"
        const val ACTION_SET_VACATION_PERIOD =
            "de.piusdischinger.timetracker.debug.SET_VACATION_PERIOD"
        const val ACTION_SET_PREF_LONG = "de.piusdischinger.timetracker.debug.SET_PREF_LONG"
        const val EXTRA_KEY = "key"
        const val EXTRA_VALUE = "value"
        const val EXTRA_URL = "url"
        const val EXTRA_USERNAME = "username"
        const val EXTRA_TOKEN = "token"
        const val EXTRA_HOST = "host"
        const val EXTRA_NAME = "name"
        const val EXTRA_NEW_NAME = "newName"
        const val EXTRA_COLOR_ID = "colorId"
        const val EXTRA_ICON = "icon"
        const val EXTRA_SAVE_RECORD = "saveRecord"
        const val EXTRA_PATH = "path"
        const val EXTRA_EVENT_ID = "eventId"
        const val EXTRA_DATE = "date"
        const val EXTRA_START = "start"
        const val EXTRA_END = "end"
        const val EXTRA_TODO_TYPE = "todoType"
        const val EXTRA_TODO_ID = "todoId"
        const val EXTRA_DONE = "done"
    }
}
