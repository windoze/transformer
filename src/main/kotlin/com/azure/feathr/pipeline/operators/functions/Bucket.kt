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
        val v = toDouble.call(listOf(arguments[0])).getDouble() ?: return NULL
        val boundaries = arguments.subList(1, arguments.size).map {
            toDouble.call(listOf(it)).getDouble()
        }
        val idx = boundaries.indexOfFirst { (it != null) && v < it }
        return Value(ColumnType.INT, if (idx < 0) boundaries.size else idx)
    }
}