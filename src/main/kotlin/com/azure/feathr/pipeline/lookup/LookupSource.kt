package com.azure.feathr.pipeline.lookup

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import com.azure.feathr.pipeline.Value
import java.util.concurrent.CompletableFuture

interface LookupSource {
    val sourceName: String

    fun getBatchSize(): Int {
        return 100
    }

    fun batchGet(keySet: Set<Value>, fields: List<String>): CompletableFuture<Map<Value, List<Value?>>> {
        // Default implementation is to get value one by one
        // Can be overridden to use batch operation if the source supports
        val futures = keySet.map { key ->
            get(key, fields).thenApply {
                key to it
            }
        }

        return CoroutineScope(SupervisorJob()).future {
            futures.chunked(getBatchSize()).forEach {
                CompletableFuture.allOf(*it.toTypedArray()).await()
            }
            futures.associate { it.await() }
        }
    }

    fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>>

    fun dump(): String {
        return sourceName
    }
}

object LookupSourceRepo {
    val lookupSources: MutableMap<String, LookupSource> = mutableMapOf()

    fun register(source: LookupSource) {
        lookupSources[source.sourceName] = source
    }

    fun contains(name: String): Boolean {
        return lookupSources.containsKey(name)
    }

    operator fun get(name: String): LookupSource {
        return lookupSources[name]!!
    }
}