package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class And : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        val ret = arguments[0].getBool() && arguments[1].getBool()
        return Value(ret)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }

    override fun dump(arguments: List<String>): String {
        return "(${arguments[0]} and ${arguments[1]})"
    }
}