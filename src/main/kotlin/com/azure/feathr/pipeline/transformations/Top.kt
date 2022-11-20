package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture
import kotlin.math.min

class Top(private val criteria: Expression, private val ascending: Boolean, private val take: Int) : Transformation {
    override fun transform(input: DataSet): DataSet {
        val keyType = criteria.getResultType(input.getColumns().map { it.type })
        if (!listOf(ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.STRING).contains(
                keyType
            )
        )
            throw IllegalType(keyType)
        return SortedDataSet(criteria, ascending, take, input)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns
    }

    override fun initialize(columns: List<Column>) {
        criteria.initialize(columns)
    }

    override fun dump(): String {
        return "top $take by ${criteria.dump()} ${if (ascending) "asc" else "desc"}"
    }

    class SortedDataSet(
        private val criteria: Expression,
        private val ascending: Boolean,
        private val take: Int,
        private val input: DataSet
    ) : DataSet {
        private var sortedDataSet: List<Row> = listOf()
        private var cursor = -1
        override fun getColumns(): List<Column> {
            return input.getColumns()
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return CoroutineScope(SupervisorJob()).future { fetchAsync(length) }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return fetch(take)
        }

        private suspend fun fetchAsync(length: Int): List<Row> {
            sort()
            if (cursor >= sortedDataSet.size) return listOf()

            val end = min(cursor + length, sortedDataSet.size)
            val ret: List<Row> = sortedDataSet.subList(cursor, end)
            cursor = end
            return ret
        }

        private suspend fun sort() {
            class CoercedComparator(private val type: ColumnType, private val ascending: Boolean) :
                Comparator<Pair<Row, Any?>> {
                override fun compare(o1: Pair<Row, Any?>, o2: Pair<Row, Any?>): Int {
                    val ret = when (type) {
                        ColumnType.INT -> (o1.second as? Int ?: 0).compareTo(o2.second as? Int ?: 0)
                        ColumnType.LONG -> (o1.second as? Long ?: 0.toLong()).compareTo(
                            o2.second as? Long ?: 0.toLong()
                        )

                        ColumnType.FLOAT -> (o1.second as? Float ?: 0.toFloat()).compareTo(
                            o2.second as? Float ?: 0.toFloat()
                        )

                        ColumnType.DOUBLE -> (o1.second as? Double ?: 0.toDouble()).compareTo(
                            o2.second as? Double ?: 0.toDouble()
                        )

                        ColumnType.STRING -> (o1.second as? String ?: "").compareTo(o2.second as? String ?: "")
                        else -> throw IllegalType(type)
                    }

                    return if (ascending) ret else -ret
                }
            }

            // TODO: Really use heap to reduce memory footprint
            if (cursor < 0) {
                sortedDataSet = input.fetchAll().await()
                    .map { it to criteria.evaluate(it).value }
                    .sortedWith(
                        CoercedComparator(
                            criteria.getResultType(input.getColumns().map { it.type }),
                            ascending
                        )
                    )
                    .map { it.first }
                    .take(take)
                cursor = 0
            }
        }

    }
}