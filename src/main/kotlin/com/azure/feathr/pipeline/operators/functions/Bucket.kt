package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import com.azure.feathr.pipeline.Value.Companion.NULL

class Bucket : Function {
    val toDouble = TypeConvertor(ColumnType.DOUBLE)
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.INT
    }

    override fun call(arguments: List<Value>): Value {
        val v = arguments[0].getDouble()
        val boundaries = arguments.subList(1, arguments.size).map {
            it.getDouble()
        }
        val idx = boundaries.indexOfFirst { v < it }
        return Value(if (idx < 0) boundaries.size else idx)
    }
}