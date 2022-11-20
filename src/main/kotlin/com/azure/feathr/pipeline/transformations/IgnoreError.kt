package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.DataSet
import com.azure.feathr.pipeline.Row
import java.util.concurrent.CompletableFuture

class IgnoreError : Transformation {
    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns
    }

    override fun transform(input: DataSet): DataSet {
        return IgnoreErrorDataSet(input)
    }

    override fun dump(): String {
        return "ignore-error"
    }

    class IgnoreErrorDataSet(private val input: DataSet) : DataSet {
        override fun getColumns(): List<Column> {
            return input.getColumns()
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return input.fetch(length).thenApply { filterError(it) }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return input.fetchAll().thenApply { filterError(it) }
        }

        private fun filterError(rows: List<Row>): List<Row> {
            return rows.filter { row ->
                row.evaluate().firstOrNull { it?.isError() ?: false } == null
            }
        }
    }
}