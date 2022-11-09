package com.azure.feathr.pipeline

import com.azure.feathr.pipeline.transformations.Transformation

class Pipeline(val inputSchema: List<Column>, private val transformations: List<Transformation>) {
    data class Stage(val transformation: Transformation, val inputSchema: List<Column>, val outputSchema: List<Column>)

    var stages: MutableList<Stage> = mutableListOf()

    val outputSchema: List<Column> get() = stages.last().outputSchema

    init {
        transformations.fold(inputSchema) { schema, t ->
            t.apply { initialize(schema) }.getOutputSchema(schema).apply {
                stages.add(Stage(t, schema, this))
            }
        }
    }

    fun process(input: DataSet): DataSet {
        // TODO: Validate input schema
        return transformations.fold(input) { ds, t ->
            t.transform(ds)
        }
    }

    // The output is still a data set as the single row may explode
    fun processSingle(input: Row): DataSet {
        // TODO: Validate input schema
        return process(EagerDataSet(inputSchema, listOf(input.evaluate())))
    }

    // The output is still a data set as the single row may explode
    fun processSingle(input: List<Any?>): DataSet {
        // TODO: Validate input schema
        return process(EagerDataSet(inputSchema, listOf(input)))
    }

    fun dump(): String {
        return "(${
            inputSchema.joinToString(", ") {
                "${it.name} as ${
                    it.type.toString().lowercase()
                }"
            }
        })" + transformations.joinToString("") { "\n| ${it.dump()}" } + "\n;"
    }
}