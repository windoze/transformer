package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture

class Where(private val criteria: Expression, private val batchSize: Int = 100) : Transformation {
    override fun transform(input: DataSet): DataSet {
        return FilteredDataSet(input, criteria, batchSize)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns
    }

    override fun initialize(columns: List<Column>) {
        criteria.initialize(columns)
    }

    override fun dump(): String {
        return "where ${criteria.dump()}"
    }

    class FilteredDataSet(private val input: DataSet, private val filter: Expression, private val batchSize: Int) :
        DataSet {
        private val cachedRows: MutableList<Row> = mutableListOf()

        override fun getColumns(): List<Column> {
            return input.getColumns()
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return CoroutineScope(SupervisorJob()).future { fetchAsync(length) }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return CoroutineScope(SupervisorJob()).future { fetchAsync(Int.MAX_VALUE) }
        }

        private suspend fun fetchAsync(length: Int): List<Row> {
            while (cachedRows.size <= length) {
                val fetched = input.fetch(batchSize).await()
                if (fetched.isEmpty()) {
                    // Upstream has been exhausted
                    break
                }
                cachedRows.addAll(fetched.filter { filter.evaluate(it).getBool() })
            }
            return if (cachedRows.size > length) {
                val ret = cachedRows.subList(0, length).toList()
                cachedRows.subList(0, length).clear()
                ret
            } else {
                val ret = cachedRows.toList()
                cachedRows.clear()
                ret
            }
        }
    }
}