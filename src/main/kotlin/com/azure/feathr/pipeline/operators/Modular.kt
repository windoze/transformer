package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.*

class Modular : Operator {
    override val arity: Int = 2 // `-1` means various length, some functions need it
    override fun apply(arguments: List<Value>): Value {
        when (arguments[0].getValueType()) {
            ColumnType.INT -> {
                val arg1: Int = arguments[0].getInt() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // INT / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.INT, arg1 % arg2)
                    }

                    ColumnType.LONG -> {
                        // INT / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.LONG, arg1.toLong() % arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.LONG -> {
                val arg1: Long = arguments[0].getLong() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // LONG / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.LONG, arg1 % arg2.toLong())
                    }

                    ColumnType.LONG -> {
                        // LONG / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.LONG, arg1 % arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            else -> throw TypeMismatch("TODO")
        }
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return when (argumentTypes[0]) {
            ColumnType.INT -> {
                when(argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.INT
                    ColumnType.LONG -> ColumnType.LONG
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.LONG -> {
                when(argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.LONG
                    ColumnType.LONG -> ColumnType.LONG
                    else -> throw TypeMismatch("TODO")
                }
            }
            else -> throw TypeMismatch("TODO")
        }
    }
    override fun dump(arguments: List<String>): String {
        return "${arguments[0]}[${arguments[1]}]"
    }
}
