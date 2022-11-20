package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.*

class Not : Operator {
    override val arity: Int = 1
    override fun apply(arguments: List<Value>): Value {
        return Value(!arguments[0].getBool())
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }

    override fun dump(arguments: List<String>): String {
        return "(not ${arguments[0]})"
    }
}