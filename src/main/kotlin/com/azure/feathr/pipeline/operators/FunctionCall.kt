package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import com.azure.feathr.pipeline.operators.functions.*
import com.azure.feathr.pipeline.operators.functions.Function
import kotlin.math.*

class FunctionCall(private val name: String) : Operator {
    override val arity: Int = -1

    private var function: Function? = null

    override fun apply(arguments: List<Value>): Value {
        return function?.call(arguments) ?: Value.NULL
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return function?.getResultType(argumentTypes) ?: ColumnType.DYNAMIC
    }

    override fun initialize(columns: List<Column>) {
        function = Function.functions[name]
    }

    override fun dump(arguments: List<String>): String {
        return "$name(${arguments.joinToString(", ")})"
    }
}

