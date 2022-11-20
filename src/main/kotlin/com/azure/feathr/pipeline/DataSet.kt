package com.azure.feathr.pipeline

import java.util.concurrent.CompletableFuture
import kotlin.math.min

enum class ValidationMode {
    STRICT,
    LENIENT,
}

interface DataSet {
    fun getColumns(): List<Column>
    fun fetch(length: Int): CompletableFuture<List<Row>>
    fun fetchAll(): CompletableFuture<List<Row>>

    fun validate(mode: ValidationMode): DataSet {
        class ValidatedDataSet(private val input: DataSet, private val mode: ValidationMode) : DataSet {
            override fun getColumns(): List<Column> {
                return input.getColumns()
            }

            override fun fetch(length: Int): CompletableFuture<List<Row>> {
                return input.fetch(length).thenApply { validateRows(it) }
            }

            override fun fetchAll(): CompletableFuture<List<Row>> {
                return input.fetchAll().thenApply { validateRows(it) }
            }

            private fun validateRow(row: Row): Row {
                val schema = input.getColumns()
                val fields = schema.zip(row.evaluate()).map { (col, field) ->
                    try {
                        when (mode) {
                            ValidationMode.STRICT -> {
                                Value(col.type.coerce(field?.value))
                            }
                            ValidationMode.LENIENT -> {
                                Value(col.type.convert(field?.value))
                            }
                        }

                    } catch (e: TransformerException) {
                        Value(e)
                    }.value
                }
                return EagerRow(schema, fields)
            }

            private fun validateRows(rows: List<Row>): List<Row> {
                return rows.map { validateRow(it) }
            }
        }

        return ValidatedDataSet(this, mode)
    }
}

// An already computed data set
class EagerDataSet(private val columns: List<Column>, private val rows: List<List<Any?>>) :
    DataSet {
    private var cursor: Int = 0

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
