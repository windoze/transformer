package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class IsNull : Operator {
    override val arity: Int = 1

    override fun apply(arguments: List<Value>): Value {
        return Value(arguments[0].isNull())
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }

    override fun dump(arguments: List<String>): String {
        return "(${arguments[0]} is null"
    }
}