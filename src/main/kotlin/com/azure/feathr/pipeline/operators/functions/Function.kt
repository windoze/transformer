package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

interface Function {
    fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }

    fun call(arguments: List<Value>): Value

    companion object {
        val functions: MutableMap<String, Function> = mutableMapOf()

        fun register(name: String, function: Function) {
            functions[name] = function
        }
    }
}

