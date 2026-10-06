package com.example.util.simpletimetracker.data_sync.work

import com.example.util.simpletimetracker.data_sync.engine.SyncEngine
import com.example.util.simpletimetracker.domain.notifications.interactor.LocalDataChangedBus
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch

/**
 * Runs a direct in process sync a short delay after local data changes
 * (timers, records, activities), so changes reach the server within a few
 * seconds. The periodic WorkManager sync remains as a background fallback;
 * this subscriber only runs while the app process is alive.
 *
 * Events generated as side effects while the engine is pulling server changes
 * are ignored to prevent ping-pong re-sync loops.
 */
@Singleton
class SyncOnDataChangeInteractor @Inject constructor(
    private val syncEngine: SyncEngine,
) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @OptIn(FlowPreview::class)
    fun subscribe() {
        scope.launch {
            LocalDataChangedBus.events
                .filter { !syncEngine.isPulling }
                .debounce(SYNC_DEBOUNCE_MS)
                .collect {
                    runCatching { syncEngine.syncNow() }
                }
        }
    }

    companion object {
        private const val SYNC_DEBOUNCE_MS = 1_000L
    }
}
