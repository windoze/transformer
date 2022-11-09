package com.azure.feathr.pipeline.lookup

import com.azure.feathr.pipeline.Value
import java.util.concurrent.CompletableFuture

class FeathrRedisSource : LookupSource {
    val name: String = ""
    val redisHost: String = ""
    val userName: String = ""
    val password: String = ""

    override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
        TODO("Not yet implemented")
    }

    override fun dump(): String {
        TODO("Not yet implemented")
    }
}