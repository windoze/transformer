package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class And : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        val ret =
            (!(Not().apply(listOf(arguments[0])).getBool() ?: true)) && (!(Not().apply(listOf(arguments[1])).getBool()
                ?: true))
        return Value(ColumnType.BOOL, ret)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }

    override fun dump(arguments: List<String>): String {
        return "(${arguments[0]} and ${arguments[1]})"
    }
}