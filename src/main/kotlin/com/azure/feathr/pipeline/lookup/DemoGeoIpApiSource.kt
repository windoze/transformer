package com.azure.feathr.pipeline.lookup

import com.azure.feathr.GlobalState
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture

class DemoGeoIpApiSource : LookupSource {
    private val webClient: WebClient = WebClient.create(GlobalState.vertx)

    override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
        val ip = key.getString() ?: ""
        return CoroutineScope(GlobalState.vertx.dispatcher()).future { getAsync(ip, fields) }
    }

    override fun dump(): String {
        return "geoip"
    }

    suspend fun getAsync(ip: String, fields: List<String>): List<Value> {
        val s = webClient.get(80, "ip-api.com", "/json/$ip")
            .send()
            .await()
            .bodyAsJsonObject()
        return fields.map {
            Value(ColumnType.STRING, s.map.getOrDefault(it, null)?.toString())
        }
    }
}