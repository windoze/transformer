package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture
import kotlin.math.min

class Take(val length: Int = 10): Transformation {
    override fun transform(input: DataSet): DataSet {
        return TakeDataSet(input, length)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns
    }

    override fun dump(): String {
        return "take $length"
    }

    data class TakeDataSet(val source: DataSet, val length: Int):
        DataSet {
        override fun getColumns(): List<Column> {
            return source.getColumns()
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return source.fetch(min(length, this.length))
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return source.fetch(length)
        }
    }
}