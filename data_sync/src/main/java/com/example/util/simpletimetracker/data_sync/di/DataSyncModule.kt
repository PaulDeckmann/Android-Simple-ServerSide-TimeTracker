package com.example.util.simpletimetracker.data_sync.di

import android.content.Context
import com.example.util.simpletimetracker.data_sync.api.LoginRequest
import com.example.util.simpletimetracker.data_sync.api.SyncApi
import com.example.util.simpletimetracker.data_sync.api.SyncConflict
import com.example.util.simpletimetracker.data_sync.api.SyncPullResponse
import com.example.util.simpletimetracker.data_sync.api.SyncPushRequest
import com.example.util.simpletimetracker.data_sync.api.SyncPushResponse
import com.example.util.simpletimetracker.data_sync.api.SyncUrlValidator
import com.example.util.simpletimetracker.data_sync.api.TokenResponse
import com.example.util.simpletimetracker.data_sync.db.SyncConflictDao
import com.example.util.simpletimetracker.data_sync.db.SyncDatabase
import com.example.util.simpletimetracker.data_sync.db.SyncIdMapDao
import com.example.util.simpletimetracker.data_sync.db.SyncStateDao
import com.example.util.simpletimetracker.data_sync.keystore.SyncCredentialStore
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.moshi.MoshiConverterFactory
import java.util.concurrent.TimeUnit
import javax.inject.Singleton

@Module
@InstallIn(SingletonComponent::class)
object DataSyncModule {

    @Provides
    @Singleton
    fun provideSyncDatabase(@ApplicationContext context: Context): SyncDatabase {
        return SyncDatabase.build(context)
    }

    @Provides
    @Singleton
    fun provideSyncStateDao(database: SyncDatabase): SyncStateDao = database.syncStateDao()

    @Provides
    @Singleton
    fun provideSyncIdMapDao(database: SyncDatabase): SyncIdMapDao = database.syncIdMapDao()

    @Provides
    @Singleton
    fun provideSyncConflictDao(database: SyncDatabase): SyncConflictDao = database.syncConflictDao()

    @Provides
    @Singleton
    fun provideMoshi(): Moshi {
        return Moshi.Builder()
            .addLast(KotlinJsonAdapterFactory())
            .build()
    }

    @Provides
    @Singleton
    fun provideOkHttpClient(): OkHttpClient {
        return OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            .build()
    }

    @Provides
    @Singleton
    fun provideSyncApi(
        moshi: Moshi,
        okHttpClient: OkHttpClient,
        credentialStore: SyncCredentialStore,
    ): SyncApi {
        val authClient = okHttpClient.newBuilder()
            .addInterceptor { chain ->
                val token = credentialStore.apiToken
                val request = chain.request().newBuilder()
                    .apply { if (token.isNotEmpty()) addHeader("Authorization", "Bearer $token") }
                    .build()
                chain.proceed(request)
            }
            .build()
        return UrlSwitchingSyncApi(
            baseUrlProvider = { credentialStore.serverUrl },
            moshi = moshi,
            okHttpClient = authClient,
        )
    }
}

class UrlSwitchingSyncApi(
    private val baseUrlProvider: () -> String,
    moshi: Moshi,
    private val okHttpClient: OkHttpClient,
) : SyncApi {

    private val moshiConverterFactory = MoshiConverterFactory.create(moshi)
    private var cachedUrl: String? = null
    private var cachedApi: SyncApi? = null
    private val lock = Any()

    private fun api(): SyncApi {
        val raw = baseUrlProvider().trim()
        if (raw.isEmpty()) throw SyncNotConfiguredException()
        val normalized = SyncUrlValidator.normalizeOrNull(raw)
            ?: throw SyncInvalidUrlException(raw)
        synchronized(lock) {
            if (cachedUrl == normalized && cachedApi != null) {
                return cachedApi!!
            }
            val retrofit = Retrofit.Builder()
                .baseUrl(normalized)
                .client(okHttpClient)
                .addConverterFactory(moshiConverterFactory)
                .build()
            val newApi = retrofit.create(SyncApi::class.java)
            cachedUrl = normalized
            cachedApi = newApi
            return newApi
        }
    }

    override suspend fun login(body: LoginRequest): TokenResponse = api().login(body)

    override suspend fun push(body: SyncPushRequest): SyncPushResponse = api().push(body)

    override suspend fun pull(since: String?): SyncPullResponse = api().pull(since)

    override suspend fun conflicts(): List<SyncConflict> = api().conflicts()
}

class SyncNotConfiguredException : IllegalStateException("Sync server is not configured")

class SyncInvalidUrlException(val url: String) : IllegalStateException("Invalid sync server URL")
