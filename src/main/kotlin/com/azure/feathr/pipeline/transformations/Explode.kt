package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture

class Explode(private val columnName: String, private val explodedType: ColumnType = ColumnType.DYNAMIC) :
    Transformation {
    override fun transform(input: DataSet): DataSet {
        return ExplodedDataSet(input, getOutputSchema(input.getColumns()), columnName)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns.map {
            if (it.name == columnName) {
                Column(it.name, explodedType)
            } else {
                it
            }
        }
    }

    override fun dump(): String {
        return "explode $columnName" + if (explodedType == ColumnType.DYNAMIC) "" else " as ${explodedType.name.lowercase()}"
    }

    class ExplodedDataSet(
        private val input: DataSet,
        private val explodedColumns: List<Column>,
        private val columnName: String
    ) : DataSet {
        private val columnIdx: Int = input.getColumns().indexOfFirst { it.name == columnName }

        override fun getColumns(): List<Column> {
            return if (columnIdx < 0) input.getColumns() else explodedColumns
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return if (columnIdx < 0)
                input.fetch(length)
            else
                CoroutineScope(SupervisorJob()).future { fetchAsync(length) }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return if (columnIdx < 0) input.fetchAll() else CoroutineScope(SupervisorJob()).future { fetchAsync(Int.MAX_VALUE) }
        }

        private suspend fun fetchAsync(length: Int): List<Row> {
            return input.fetch(length).await().flatMap { r ->
                val row = r.evaluate()
                val array = row[columnIdx]?.getArray() ?: listOf()
                array.map {
                    val newRow = row.map { it?.getDynamic() }.toMutableList()
                    newRow[columnIdx] = it
                    EagerRow(explodedColumns, newRow)
                }
            }
        }
    }
}