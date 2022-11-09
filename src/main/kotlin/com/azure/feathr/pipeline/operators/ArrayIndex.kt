package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.IllegalValue
import com.azure.feathr.pipeline.IndexOutOfBound
import com.azure.feathr.pipeline.Value

class ArrayIndex : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        assert(arguments.size == arity)
        val idx = arguments[1].getInt() ?: throw IllegalValue(null)
        val v =  (arguments[0].getArray() ?: throw IllegalValue(null))[idx] ?: throw IndexOutOfBound(idx)
        return Value(ColumnType.DYNAMIC, v)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }
}