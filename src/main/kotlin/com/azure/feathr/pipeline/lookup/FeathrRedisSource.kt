package com.azure.feathr.pipeline.lookup

import com.azure.feathr.GlobalState
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import com.fasterxml.jackson.annotation.JsonIgnore
import com.linkedin.feathr.common.types.protobuf.FeatureValueOuterClass.FeatureValue
import io.vertx.core.net.NetClientOptions
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.future
import java.util.*
import java.util.concurrent.CompletableFuture

class FeathrRedisSource(
    val name: String = "",
    val host: String = "",
    val ssl: Boolean = true,
    val password: String = "",
    val table: String = ""
) : LookupSource {
    private val client: Redis by lazy {
        Redis.createClient(
            GlobalState.vertx,
            RedisOptions()
                .setEndpoints(listOf(host))
                .setPassword(password)
                .setNetClientOptions(NetClientOptions().setSsl(ssl))
        )
    }

    private val redis: RedisAPI by lazy {
        RedisAPI.api(client)
    }

    override val sourceName: String
        get() = name

    override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
        // No key, no value
        val k = key.getString()
        if (k.isNullOrBlank())
            return CompletableFuture.completedFuture(List(fields.size) { Value(ColumnType.DYNAMIC, null) })
        return CoroutineScope(GlobalState.vertx.dispatcher()).future {
            getAsync(k, fields)
        }
    }

    override fun batchGet(keySet: Set<Value>, fields: List<String>): CompletableFuture<Map<Value, List<Value?>>> {
        val ks = keySet.mapNotNull {
            it.getString()
        }.toSet()
        return CoroutineScope(GlobalState.vertx.dispatcher()).future {
            batchGetAsync(ks, fields)
        }
    }

    private suspend fun getAsync(key: String, fields: List<String>): List<Value?> {
        val ret = redis.hmget(listOf(constructKey(key)) + fields)

        return parseResponse(ret.await(), fields)
    }

    private suspend fun batchGetAsync(keySet: Set<String>, fields: List<String>): Map<Value, List<Value?>> {
        val keyList = keySet.toList()
        val cmdArg = fields.toTypedArray()
        val batch = keyList.map {
            Request.cmd(Command.HMGET, constructKey(it), *cmdArg)
        }
        val responses = client.batch(batch).await()
        return keyList.zip(responses).associate { (k, r) ->
            Value(ColumnType.STRING, k) to parseResponse(r, fields)
        }
    }

    private fun parseResponse(resp: Response, fields: List<String>): List<Value?> {
        val featureValues = resp.toList().map {
            FeatureValue.parseFrom(Base64.getDecoder().decode(it.toString()))
        }
        return featureValues.map { featureValueToValue(it) }
    }

    private fun featureValueToValue(fv: FeatureValue): Value {
        if (fv.hasBooleanValue()) {
            return Value(ColumnType.BOOL, fv.booleanValue)
        }
        if (fv.hasIntValue()) {
            return Value(ColumnType.INT, fv.intValue)
        }
        if (fv.hasLongValue()) {
            return Value(ColumnType.LONG, fv.longValue)
        }
        if (fv.hasFloatArray()) {
            return Value(ColumnType.FLOAT, fv.floatValue)
        }
        if (fv.hasDoubleArray()) {
            return Value(ColumnType.DOUBLE, fv.doubleValue)
        }
        if (fv.hasStringValue()) {
            return Value(ColumnType.STRING, fv.stringValue)
        }
        if (fv.hasBooleanArray()) {
            return Value(ColumnType.ARRAY, fv.booleanArray.booleansList.toList())
        }
        if (fv.hasIntArray()) {
            return Value(ColumnType.ARRAY, fv.intArray.integersList.toList())
        }
        if (fv.hasLongArray()) {
            return Value(ColumnType.ARRAY, fv.longArray.longsList.toList())
        }
        if (fv.hasFloatArray()) {
            return Value(ColumnType.ARRAY, fv.floatArray.floatsList.toList())
        }
        if (fv.hasDoubleArray()) {
            return Value(ColumnType.ARRAY, fv.doubleArray.doublesList.toList())
        }
        if (fv.hasStringArray()) {
            return Value(ColumnType.ARRAY, fv.stringArray.stringsList.toList())
        }
        if (fv.hasSparseBoolArray()) {
            return Value(ColumnType.ARRAY, fv.sparseBoolArray.valueBooleansList.toList())
        }
        if (fv.hasSparseIntegerArray()) {
            return Value(ColumnType.ARRAY, fv.sparseIntegerArray.valueIntegersList.toList())
        }
        if (fv.hasSparseLongArray()) {
            return Value(ColumnType.ARRAY, fv.sparseLongArray.valueLongsList.toList())
        }
        if (fv.hasSparseFloatArray()) {
            return Value(ColumnType.ARRAY, fv.sparseFloatArray.valueFloatsList.toList())
        }
        if (fv.hasSparseDoubleArray()) {
            return Value(ColumnType.ARRAY, fv.sparseDoubleArray.valueDoublesList.toList())
        }
        if (fv.hasSparseStringArray()) {
            return Value(ColumnType.ARRAY, fv.sparseStringArray.valueStringsList.toList())
        }
        TODO("Unsupported feature type")
    }

    private fun constructKey(key: String): String {
        return "$table$KEY_SEPARATOR$key"
    }

    companion object {
        const val KEY_SEPARATOR: String = ":"
    }
}