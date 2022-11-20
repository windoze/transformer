package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.IllegalValue
import com.azure.feathr.pipeline.KeyNotFound
import com.azure.feathr.pipeline.Value

class MapIndex : Operator {
    override val arity: Int = 2

    override fun apply(arguments: List<Value>): Value {
        assert(arguments.size == arity)
        val key = arguments[1].getString()
        val v = (arguments[0].getObject())[key] ?: throw KeyNotFound(key)
        return Value(v)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }

    override fun dump(arguments: List<String>): String {
        return "${arguments[0]}.${arguments[1]}"
    }
}