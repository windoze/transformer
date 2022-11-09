package com.azure.feathr.pipeline

import java.util.concurrent.CompletableFuture
import kotlin.math.min

interface DataSet {
    fun getColumns(): List<Column>
    fun fetch(length: Int): CompletableFuture<List<Row>>
    fun fetchAll(): CompletableFuture<List<Row>>
}

// An already computed data set
class EagerDataSet(private val columns: List<Column>, private val rows: List<List<Any?>>) :
    DataSet {
    var cursor: Int = 0

    override fun getColumns(): List<Column> {
        return columns
    }

    /**
     * NOTE: length is only a hint, returned row list could be longer or shorter than it
     */
    override fun fetch(length: Int): CompletableFuture<List<Row>> {
        if (cursor >= rows.size) return CompletableFuture.completedFuture(listOf())

        val end = min(cursor + length, rows.size)
        val ret: CompletableFuture<List<Row>> =
            CompletableFuture.completedFuture(rows.subList(cursor, end).map {
                EagerRow(columns, it)
            })
        cursor = end
        return ret
    }

    override fun fetchAll(): CompletableFuture<List<Row>> {
        return fetch(rows.size)
    }
}
