package com.azure.feathr.pipeline.lookup

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import com.azure.feathr.pipeline.Value
import java.util.concurrent.CompletableFuture

interface LookupSource {
    fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>>
    fun batchGet(keySet: Set<Value>, fields: List<String>): CompletableFuture<Map<Value, List<Value?>>> {
        // Default implementation is to get value one by one
        // Can be overridden to use batch operation if the source supports
        return CoroutineScope(SupervisorJob()).future {
            keySet.associateWith {
                get(it, fields).await()
            }
        }
    }

    fun dump(): String
}

object LookupSourceRepo {
    private val lookupSources: MutableMap<String, LookupSource> = mutableMapOf()

    fun register(name: String, source: LookupSource) {
        lookupSources[name] = source
    }

    operator fun get(name: String): LookupSource {
        return lookupSources[name]!!
    }
}