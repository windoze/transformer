package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.*

class Divide : Operator {
    override val arity: Int = 2 // `-1` means various length, some functions need it
    override fun apply(arguments: List<Value>): Value {
        when (arguments[0].getValueType()) {
            ColumnType.INT -> {
                val arg1: Int = arguments[0].getInt() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // INT / INT
                        return Value(arguments[0].getInt() / arguments[1].getInt())
                    }

                    ColumnType.LONG -> {
                        // INT / LONG
                        return Value(arguments[0].getLong() / arguments[1].getLong())
                    }

                    ColumnType.FLOAT -> {
                        // INT / FLOAT
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // INT / DOUBLE
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.LONG -> {
                val arg1: Long = arguments[0].getLong() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // LONG / INT
                        return Value(arguments[0].getLong() / arguments[1].getLong())
                    }

                    ColumnType.LONG -> {
                        // LONG / LONG
                        return Value(arguments[0].getLong() / arguments[1].getLong())
                    }

                    ColumnType.FLOAT -> {
                        // LONG / FLOAT
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // LONG / DOUBLE
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.FLOAT -> {
                val arg1: Float = arguments[0].getFloat() ?: 0f
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // FLOAT / INT
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.LONG -> {
                        // FLOAT / LONG
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.FLOAT -> {
                        // FLOAT / FLOAT
                        return Value(arguments[0].getFloat() / arguments[1].getFloat())
                    }

                    ColumnType.DOUBLE -> {
                        // FLOAT / DOUBLE
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.DOUBLE -> {
                val arg1: Double = arguments[0].getDouble() ?: 0.0
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // DOUBLE / INT
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.LONG -> {
                        // DOUBLE / LONG
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.FLOAT -> {
                        // DOUBLE / FLOAT
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // DOUBLE / DOUBLE
                        return Value(arguments[0].getDouble() / arguments[1].getDouble())
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
                when (argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.INT
                    ColumnType.LONG -> ColumnType.LONG
                    ColumnType.FLOAT -> ColumnType.DOUBLE
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.LONG -> {
                when (argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.LONG
                    ColumnType.LONG -> ColumnType.LONG
                    ColumnType.FLOAT -> ColumnType.DOUBLE
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.FLOAT -> {
                when (argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.DOUBLE
                    ColumnType.LONG -> ColumnType.DOUBLE
                    ColumnType.FLOAT -> ColumnType.FLOAT
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.DOUBLE -> {
                when (argumentTypes[1]) {
                    ColumnType.INT -> ColumnType.DOUBLE
                    ColumnType.LONG -> ColumnType.DOUBLE
                    ColumnType.FLOAT -> ColumnType.DOUBLE
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }

            else -> throw TypeMismatch("TODO")
        }
    }

    override fun dump(arguments: List<String>): String {
        return "${arguments[0]} / ${arguments[1]}"
    }
}
