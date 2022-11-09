package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class Split : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.ARRAY
    }

    override fun call(arguments: List<Value>): Value {
        val str = arguments[0].getString() ?: return Value(ColumnType.ARRAY, listOf<Any>())
        val sep = arguments[1].getString() ?: return Value(ColumnType.ARRAY, listOf<Any>())
        return Value(ColumnType.ARRAY, str.split(sep))
    }
}