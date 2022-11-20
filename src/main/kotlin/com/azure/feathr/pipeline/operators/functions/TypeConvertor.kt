package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TypeMismatch
import com.azure.feathr.pipeline.Value

class TypeConvertor(private val type: ColumnType) : Function {
    override fun call(arguments: List<Value>): Value {
        val argument = arguments[0]
        return Value(type.convert(argument.value))
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return type
    }
}