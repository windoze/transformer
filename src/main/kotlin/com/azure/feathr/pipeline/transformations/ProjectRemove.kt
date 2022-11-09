package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture

class ProjectRemove(private val removed: List<String>) : Transformation {
    override fun transform(input: DataSet): DataSet {
        return ProjectedDataSet(removed, input)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        val projections: List<Pair<Int, Column>> =
            inputColumns.mapIndexed { idx, col ->
                Pair(idx, col)
            }.filter { (_, col) ->
                !removed.contains(col.name)
            }

        return projections.map { (_, col) -> col }
    }

    override fun dump(): String {
        return "project-remove ${removed.joinToString(",")}"
    }

    class ProjectedDataSet(private val removed: List<String>, private val input: DataSet) :
        DataSet {
        private val projections: List<Pair<Int, Column>> =
            input.getColumns().mapIndexed { idx, col ->
                Pair(idx, col)
            }.filter { (_, col) ->
                !removed.contains(col.name)
            }

        private val projectedColumns = projections.map { (_, col) -> col }

        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return input.fetch(length).thenApply {
                it.map { col ->
                    ProjectedRow(projectedColumns, projections, col)
                }
            }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return input.fetchAll().thenApply {
                it.map { col ->
                    ProjectedRow(projectedColumns, projections, col)
                }
            }
        }
    }

    class ProjectedRow(
        private val projectedColumns: List<Column>,
        private val projections: List<Pair<Int, Column>>,
        private val input: Row
    ) :
        Row {
        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun getColumn(index: Int): Value {
            val origIdx = projections[index].first
            return input.getColumn(origIdx)
        }
    }
}