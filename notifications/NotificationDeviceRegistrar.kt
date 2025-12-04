package com.example.sportsstatsx.notifications

import android.content.Context
import android.provider.Settings
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.TimeZone

object NotificationDeviceRegistrar {

    private const val TAG = "SportsStatsX-NotiReg"
    // Render 메인 API 엔드포인트 (이미 curl 에서 쓰는 주소)
    private const val BASE_URL = "https://sports-api-8vlh.onrender.com"

    /**
     * FCM 토큰을 서버 /api/notifications/register_device 로 보내서
     * notification_devices 테이블에 등록 / 업데이트.
     */
    suspend fun register(context: Context, fcmToken: String) {
        val appContext = context.applicationContext

        // 안드로이드 고유 device id (A방식: 서버에서만 사용)
        val deviceId = Settings.Secure.getString(
            appContext.contentResolver,
            Settings.Secure.ANDROID_ID
        ) ?: run {
            Log.w(TAG, "ANDROID_ID is null, skip registration")
            return
        }

        val tzId = TimeZone.getDefault().id

        val appVersion = try {
            val pm = appContext.packageManager
            val pInfo = pm.getPackageInfo(appContext.packageName, 0)
            pInfo.versionName ?: ""
        } catch (e: Exception) {
            Log.w(TAG, "Failed to get app version", e)
            ""
        }

        val body = JSONObject().apply {
            put("device_id", deviceId)
            put("fcm_token", fcmToken)
            put("platform", "android")
            put("app_version", appVersion)
            put("timezone", tzId)
        }

        withContext(Dispatchers.IO) {
            var conn: HttpURLConnection? = null
            try {
                val url = URL("$BASE_URL/api/notifications/register_device")
                conn = (url.openConnection() as HttpURLConnection).apply {
                    requestMethod = "POST"
                    connectTimeout = 5000
                    readTimeout = 5000
                    doOutput = true
                    setRequestProperty("Content-Type", "application/json; charset=utf-8")
                }

                // JSON body 전송
                conn.outputStream.use { os ->
                    val bytes = body.toString().toByteArray(Charsets.UTF_8)
                    os.write(bytes)
                }

                val code = conn.responseCode
                val stream =
                    if (code in 200..299) conn.inputStream else conn.errorStream

                val resp = stream?.use { s ->
                    BufferedReader(InputStreamReader(s)).readText()
                }

                Log.d(TAG, "register_device response: code=$code, body=$resp")
            } catch (e: Exception) {
                Log.e(TAG, "Failed to register device", e)
            } finally {
                conn?.disconnect()
            }
        }
    }
}
