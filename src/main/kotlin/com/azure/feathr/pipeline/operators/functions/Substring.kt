package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class Substring : Function {
    override fun call(arguments: List<Value>): Value {
        val s = arguments[0].getString() ?: ""
        val start = arguments[1].getInt() ?: 0
        val len = if (arguments.size > 2) arguments[2].getInt() ?: -1 else -1
        val ret = if (len < 0) s.substring(start) else s.substring(start, start + len)
        return Value(ColumnType.STRING, ret)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.STRING
    }
}