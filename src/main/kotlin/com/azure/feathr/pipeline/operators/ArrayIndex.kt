package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.IllegalValue
import com.azure.feathr.pipeline.IndexOutOfBound
import com.azure.feathr.pipeline.Value

class ArrayIndex : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        assert(arguments.size == arity)
        val idx = arguments[1].getInt()
        val v =  arguments[0].getArray()[idx] ?: throw IndexOutOfBound(idx)
        return Value(v)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }

    override fun dump(arguments: List<String>): String {
        return "(${arguments[0]}[${arguments[1]}])"
    }
}