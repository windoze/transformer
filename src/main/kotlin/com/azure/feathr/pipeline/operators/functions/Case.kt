package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class Case : Function {
    override fun call(arguments: List<Value>): Value {
        arguments.chunked(2).forEach { case ->
            if (case.size == 2) {
                if (case[0].getBool() == true) return@call case[1]
            } else {
                return@call case[0]
            }
        }

        return Value(ColumnType.DYNAMIC, null)
    }
}