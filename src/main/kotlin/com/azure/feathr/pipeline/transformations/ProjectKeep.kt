package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.DataSet
import com.azure.feathr.pipeline.Row
import com.azure.feathr.pipeline.Value
import java.util.concurrent.CompletableFuture

class ProjectKeep(private val kept: List<String>) : Transformation {
    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        val projections: List<Pair<Int, Column>> =
            inputColumns.mapIndexed { idx, col ->
                Pair(idx, col)
            }.filter { (_, col) ->
                kept.contains(col.name)
            }

        return projections.map { (_, col) -> col }
    }

    override fun transform(input: DataSet): DataSet {
        return ProjectedDataSet(kept, input)
    }

    override fun dump(): String {
        return "project-keep ${kept.joinToString(", ")}"
    }

    class ProjectedDataSet(private val kept: List<String>, private val input: DataSet) : DataSet {
        private val projections: List<Pair<Int, Column>> =
            input.getColumns().mapIndexed { idx, col ->
                Pair(idx, col)
            }.filter { (_, col) ->
                kept.contains(col.name)
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