package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.*
import java.util.concurrent.CompletableFuture

// TODO: Handle duplicated names
class Project(private val addition: List<Pair<String, Expression>>) : Transformation {
    override fun transform(input: DataSet): DataSet {
        return ProjectedDataSet(input, getOutputSchema(input.getColumns()), addition)
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns + addition.map { (name, exp) ->
            Column(name, exp.getResultType(inputColumns.map { it.type }))
        }.toList()
    }

    override fun initialize(columns: List<Column>) {
        addition.forEach { (_, exp) -> exp.initialize(columns) }
    }

    override fun dump(): String {
        return "project ${addition.joinToString(", ") { "${it.first}=${it.second.dump()}" }}"
    }

    class ProjectedDataSet(
        private val input: DataSet,
        private val projectedColumns: List<Column>,
        addition: List<Pair<String, Expression>>
    ) : DataSet {
        private val inputColumns: List<Column> = input.getColumns()

        private val calculations: List<Expression> =
            List(inputColumns.size) { idx ->
                GetColumnByIndex(idx)
            } + addition.map { (_, exp) ->
                exp
            }

        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return input.fetch(length).thenApply { rows ->
                rows.map {
                    ProjectedRow(projectedColumns, calculations, it)
                }
            }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return input.fetchAll().thenApply { rows ->
                rows.map {
                    ProjectedRow(projectedColumns, calculations, it)
                }
            }
        }
    }

    class ProjectedRow(
        private val projectedColumns: List<Column>,
        private val calculations: List<Expression>,
        private val input: Row,
    ) : Row {
        override fun getColumns(): List<Column> {
            return projectedColumns
        }

        override fun getColumn(index: Int): Value {
            return calculations[index].evaluate(input)
        }
    }
}