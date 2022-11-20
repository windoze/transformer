package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TypeMismatch
import com.azure.feathr.pipeline.Value

class Len : Function {
    override fun call(arguments: List<Value>): Value {
        val len = when (val v = arguments[0].value) {
            null -> 0
            is String -> v.length
            is List<*> -> v.size
            is Map<*, *> -> v.size
            else -> throw TypeMismatch("TODO")
        }
        return Value(len)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.INT
    }
}