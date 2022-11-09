package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class MakeArray : Function {
    override fun call(arguments: List<Value>): Value {
        return Value(ColumnType.ARRAY, arguments.map { it.getDynamic() })
    }
}