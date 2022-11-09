package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture

class ProjectRename(private val renamed: Map<String, String>) : Transformation {
    override fun transform(input: DataSet): DataSet {
        return ProjectedDataSet(input, getOutputSchema(input.getColumns()))
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns.map {
            Column(renamed.getOrDefault(it.name, it.name), it.type)
        }
    }

    override fun dump(): String {
        return "project-rename ${renamed.toList().joinToString(",") { "${it.first}=${it.second}" }}"
    }

    class ProjectedDataSet(
        private val input: DataSet,
        private val projectedColumns: List<Column>
    ) : DataSet {
        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return input.fetch(length).thenApply {
                it.map { col ->
                    ProjectedRow(projectedColumns, col)
                }
            }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return input.fetchAll().thenApply {
                it.map { col ->
                    ProjectedRow(projectedColumns, col)
                }
            }
        }
    }

    class ProjectedRow(
        private val projectedColumns: List<Column>,
        private val input: Row
    ) :
        Row {
        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun getColumn(index: Int): Value {
            return input.getColumn(index)
        }
    }
}