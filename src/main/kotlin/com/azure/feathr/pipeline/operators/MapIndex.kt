package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.IllegalValue
import com.azure.feathr.pipeline.KeyNotFound
import com.azure.feathr.pipeline.Value

class MapIndex : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        assert(arguments.size == arity)
        val key = arguments[1].getString() ?: throw IllegalValue(null)
        val v = (arguments[0].getObject() ?: throw IllegalValue(null))[key] ?: throw KeyNotFound(key)
        return Value(ColumnType.DYNAMIC, v)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }
}