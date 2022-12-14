package com.azure.feathr.pipeline.lookup

import com.azure.feathr.Main
import com.azure.feathr.online.VertxWebException
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.HttpError
import com.azure.feathr.pipeline.LookupSourceError
import com.azure.feathr.pipeline.Value
import com.fasterxml.jackson.annotation.JsonIgnore
import com.noenv.jsonpath.JsonPath
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture

data class HttpJsonApiSource(
    val name: String = "",
    val urlBase: String = "",
    val method: String = "POST",
    val additionalHeaders: Map<String, String> = mapOf(),
    val additionalQueryParams: Map<String, String> = mapOf(),

    // Key in URL, `$` is the placeholder of the key, this template doesn't include URL base
    val keyUrlTemplate: String = "",

    // Key in header
    val keyHeader: String = "",
    val keyHeaderTemplate: String = "",

    // Key in query param
    val keyQueryParam: String = "",

    // Key in JSON payload
    val requestTemplate: JsonObject = JsonObject(),
    // Location of the key in the request JSON payload
    val keyPath: String = "",

    // TODO: Authentication

    // How to find each result fields
    val resultPath: Map<String, String> = mapOf(),
) : LookupSource {
    private val client by lazy {
        WebClient.create(Main.vertx)
    }

    override val sourceName: String
        @JsonIgnore
        get() = name

    override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
        val k = key.value ?: return CompletableFuture.completedFuture(List(fields.size) {
            Value(null)
        })
        return CoroutineScope(Main.vertx.dispatcher()).future { requestAsync(k, fields) }
    }

    private suspend fun requestAsync(key: Any, fields: List<String>): List<Value> {
        val url = if (keyUrlTemplate.isNotBlank()) {
            getSecret(urlBase) + keyUrlTemplate.replace("$", key.toString())
        } else {
            getSecret(urlBase)
        }

        val request = client.requestAbs(getMethod(), url)

        if (keyQueryParam.isNotBlank()) {
            request.addQueryParam(keyQueryParam, key.toString())
        }

        for ((k, v) in additionalQueryParams) {
            request.addQueryParam(k, getSecret(v))
        }

        additionalHeaders.forEach { (h, v) ->
            request.putHeader(h, getSecret(v))
        }

        if (keyHeader.isNotBlank()) {
            if (keyHeaderTemplate.isBlank()) {
                request.putHeader(keyHeader, key.toString())
            } else {
                request.putHeader(keyHeader, keyHeaderTemplate.replace("$", key.toString()))
            }
        }

        try {
            val resp = if (!requestTemplate.isEmpty) {
                val payload = requestTemplate.copy()
                if (keyPath.isNotBlank()) {
                    setValue(payload, keyPath, key)
                }
                request.sendJsonObject(payload)
            } else {
                request.send()
            }.await().apply {
                if (statusCode() >= 400) {
                    throw HttpError(statusCode(), bodyAsString())
                }
            }.bodyAsJsonObject()

            return fields.map { field ->
                Value(resultPath[field]?.let {
                    getValue(resp, it)
                })
            }
        } catch (e: Throwable) {
            return List(fields.size) { Value(LookupSourceError(e)) }
        }
    }

    private fun getMethod(): HttpMethod {
        return when (method.uppercase()) {
            "GET" -> HttpMethod.GET
            "POST" -> HttpMethod.POST
            else -> TODO("Unsupported HTTP method $method")
        }
    }

    private fun getValue(o: JsonObject, path: String): Any? {
        return convJsonValue(JsonPath.getValue(o, path))
    }

    private fun setValue(o: JsonObject, path: String, value: Any) {
        JsonPath.put(o, path, value)
    }

    private fun convJsonValue(v: Any?): Any? {
        if (v == null) return null
        return when (v) {
            is Boolean -> v
            is Int -> v
            is Long -> v
            is Float -> v
            is Double -> v
            is String -> v
            is JsonArray -> {
                v.map {
                    convJsonValue(it)
                }.toList()
            }

            is JsonObject -> {
                v.associate { (k, v) ->
                    k.toString() to convJsonValue(v)
                }
            }

            else -> TODO("Unknown value $v")
        }
    }
}