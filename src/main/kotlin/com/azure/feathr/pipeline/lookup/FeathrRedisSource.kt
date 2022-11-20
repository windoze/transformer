package com.azure.feathr.pipeline.lookup

import com.azure.feathr.Main
import com.azure.feathr.pipeline.*
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.linkedin.feathr.common.types.protobuf.FeatureValueOuterClass.FeatureValue
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import java.util.*
import java.util.concurrent.CompletableFuture

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(value = ["port"], allowSetters = true)
class FeathrRedisSource(
    private val name: String = "",
    private val host: String = "",
    private val port: Int = 6379,
    private val ssl: Boolean = true,
    private val password: String = "",
    private val table: String = ""
) : LookupSource {
    @delegate:JsonIgnore
    @get:JsonIgnore
    private val client: RedisClient by lazy {
        val host = getSecret(host)
        val proto = if (ssl) "rediss" else "redis"
        val h = if (port == 6379) host else "$host:$port"
        val endpoint = if (password.isBlank()) {
            h
        } else {
            "${getSecret(password)}@$h"
        }
        RedisClient.create("$proto://$endpoint")
    }

    @delegate:JsonIgnore
    @get:JsonIgnore
    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private val api by lazy {
        client.connect().coroutines()
    }

    override val sourceName: String
        @JsonIgnore
        get() = name

    @JsonIgnore
    override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
        // No key, no value
        val k = key.getString()
        if (k.isBlank())
            return CompletableFuture.completedFuture(List(fields.size) { Value(null) })
        return CoroutineScope(Main.vertx.dispatcher()).future {
            getAsync(k, fields)
        }
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun getAsync(key: String, fields: List<String>): List<Value?> {
        val ret: MutableList<String> = mutableListOf()
        return try {
            api.hmget(constructKey(key), *fields.toTypedArray()).collect {
                ret.add(it.value)
            }

            parseResponse(ret)
        } catch (e: TransformerException) {
            List(fields.size) { Value(e) }
        } catch (e: Throwable) {
            List(fields.size) { Value(LookupSourceError(e)) }
        }
    }

    private fun parseResponse(resp: List<String>): List<Value?> {
        val featureValues = resp.toList().map {
            FeatureValue.parseFrom(Base64.getDecoder().decode(it))
        }
        return featureValues.map { featureValueToValue(it) }
    }

    private fun featureValueToValue(fv: FeatureValue): Value {
        if (fv.hasBooleanValue()) {
            return Value(fv.booleanValue)
        }
        if (fv.hasIntValue()) {
            return Value(fv.intValue)
        }
        if (fv.hasLongValue()) {
            return Value(fv.longValue)
        }
        if (fv.hasFloatValue()) {
            return Value(fv.floatValue)
        }
        if (fv.hasDoubleValue()) {
            return Value(fv.doubleValue)
        }
        if (fv.hasStringValue()) {
            return Value(fv.stringValue)
        }
        if (fv.hasBooleanArray()) {
            return Value(fv.booleanArray.booleansList.toList())
        }
        if (fv.hasByteArray()) {
            // ByteArray is array of ByteString, or byte sequence
            // So logically it's an array of array of bytes
            return Value(
                fv.byteArray.bytesList.map { it.map { byte -> byte.toInt() }.toList() }.toList()
            )
        }
        if (fv.hasIntArray()) {
            return Value(fv.intArray.integersList.toList())
        }
        if (fv.hasLongArray()) {
            return Value(fv.longArray.longsList.toList())
        }
        if (fv.hasFloatArray()) {
            return Value(fv.floatArray.floatsList.toList())
        }
        if (fv.hasDoubleArray()) {
            return Value(fv.doubleArray.doublesList.toList())
        }
        if (fv.hasStringArray()) {
            return Value(fv.stringArray.stringsList.toList())
        }
        if (fv.hasSparseBoolArray()) {
            return Value(fv.sparseBoolArray.valueBooleansList.toList())
        }
        if (fv.hasSparseIntegerArray()) {
            return Value(fv.sparseIntegerArray.valueIntegersList.toList())
        }
        if (fv.hasSparseLongArray()) {
            return Value(fv.sparseLongArray.valueLongsList.toList())
        }
        if (fv.hasSparseFloatArray()) {
            return Value(fv.sparseFloatArray.valueFloatsList.toList())
        }
        if (fv.hasSparseDoubleArray()) {
            return Value(fv.sparseDoubleArray.valueDoublesList.toList())
        }
        if (fv.hasSparseStringArray()) {
            return Value(fv.sparseStringArray.valueStringsList.toList())
        }
        throw UnsupportedFeatureType("Unsupported feature type")
    }

    private fun constructKey(key: String): String {
        return "${getSecret(table)}$KEY_SEPARATOR$key"
    }

    companion object {
        const val KEY_SEPARATOR: String = ":"
    }
}