package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Initializable
import com.azure.feathr.pipeline.Value

interface Operator : Initializable {
    val arity: Int
    fun apply(arguments: List<Value>): Value

    fun getResultType(argumentTypes: List<ColumnType>): ColumnType

    fun dump(): String {
        return this.javaClass.simpleName
    }
}