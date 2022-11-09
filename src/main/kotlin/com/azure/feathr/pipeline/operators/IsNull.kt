package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class IsNull : Operator {
    override val arity: Int = 1

    override fun apply(arguments: List<Value>): Value {
        return Value(ColumnType.BOOL, arguments[0].getDynamic() == null)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }
}