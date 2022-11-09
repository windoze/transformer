package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.*

class Not : Operator {
    override val arity: Int = 1
    override fun apply(arguments: List<Value>): Value {
        val arg = arguments[0]
        return when (arg.getValueType()) {
            ColumnType.BOOL -> Value(ColumnType.BOOL, (arg.getBool() ?: false).not())

            ColumnType.INT -> Value(ColumnType.BOOL, (arg.getInt() ?: 0) == 0)

            ColumnType.LONG -> Value(ColumnType.BOOL, (arg.getLong() ?: 0L) == 0L)

            ColumnType.FLOAT -> Value(ColumnType.BOOL, (arg.getFloat() ?: 0.0) == 0.0)

            ColumnType.DOUBLE -> Value(ColumnType.BOOL, (arg.getDouble() ?: 0.0) == 0.0)

            ColumnType.STRING -> Value(ColumnType.BOOL, (arg.getString() ?: "").isEmpty())

            ColumnType.ARRAY -> Value(ColumnType.BOOL, (arg.getArray() ?: listOf()).isEmpty())

            ColumnType.OBJECT -> Value(ColumnType.BOOL, (arg.getObject() ?: mapOf()).isEmpty())

            ColumnType.DYNAMIC -> Value(ColumnType.BOOL, arg.getObject() == null)
        }
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }
}