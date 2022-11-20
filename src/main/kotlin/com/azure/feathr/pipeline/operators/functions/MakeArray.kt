package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class MakeArray : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.ARRAY
    }

    override fun call(arguments: List<Value>): Value {
        return Value(arguments.map { it.value })
    }
}