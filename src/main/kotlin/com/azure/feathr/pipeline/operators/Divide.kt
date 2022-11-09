package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.*

class Divide : Operator {
    override val arity: Int = 2 // `-1` means various length, some functions need it
    override fun apply(arguments: List<Value>): Value {
        when (arguments[0].getValueType()) {
            ColumnType.BOOL -> {
                val arg1: Int = if (arguments[0].getBool() == true) 1 else 0
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // BOOL / BOOL
                        val arg2: Int = if (arguments[1].getBool() == true) 1 else 0
                        return Value(ColumnType.INT, arg1 / arg2)
                    }

                    ColumnType.INT -> {
                        // BOOL / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.INT, arg1 / arg2)
                    }

                    ColumnType.LONG -> {
                        // BOOL / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.LONG, arg1.toLong() / arg2)
                    }

                    ColumnType.FLOAT -> {
                        // BOOL / FLOAT
                        val arg2: Float = arguments[1].getFloat() ?: 0.0f
                        return Value(ColumnType.FLOAT, arg1.toFloat() / arg2)
                    }

                    ColumnType.DOUBLE -> {
                        // BOOL / DOUBLE
                        val arg2: Double = arguments[1].getDouble() ?: 0.0
                        return Value(ColumnType.DOUBLE, arg1.toDouble() / arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.INT -> {
                val arg1: Int = arguments[0].getInt() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // INT / BOOL
                        val arg2: Int = if (arguments[1].getBool() == true) 1 else 0
                        return Value(ColumnType.INT, arg1 / arg2)
                    }

                    ColumnType.INT -> {
                        // INT / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.INT, arg1 / arg2)
                    }

                    ColumnType.LONG -> {
                        // INT / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.LONG, arg1.toLong() / arg2)
                    }

                    ColumnType.FLOAT -> {
                        // INT / FLOAT
                        val arg2: Float = arguments[1].getFloat() ?: 0.0f
                        return Value(ColumnType.FLOAT, arg1.toFloat() / arg2)
                    }

                    ColumnType.DOUBLE -> {
                        // INT / DOUBLE
                        val arg2: Double = arguments[1].getDouble() ?: 0.0
                        return Value(ColumnType.DOUBLE, arg1.toDouble() / arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.LONG -> {
                val arg1: Long = arguments[0].getLong() ?: 0
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // LONG / BOOL
                        val arg2: Int = if (arguments[1].getBool() == true) 1 else 0
                        return Value(ColumnType.LONG, arg1 / arg2.toLong())
                    }

                    ColumnType.INT -> {
                        // LONG / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.LONG, arg1 / arg2.toLong())
                    }

                    ColumnType.LONG -> {
                        // LONG / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.LONG, arg1 / arg2)
                    }

                    ColumnType.FLOAT -> {
                        // LONG / FLOAT
                        val arg2: Float = arguments[1].getFloat() ?: 0.0f
                        return Value(ColumnType.FLOAT, arg1.toFloat() / arg2)
                    }

                    ColumnType.DOUBLE -> {
                        // LONG / DOUBLE
                        val arg2: Double = arguments[1].getDouble() ?: 0.0
                        return Value(ColumnType.DOUBLE, arg1.toDouble() / arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.FLOAT -> {
                val arg1: Float = arguments[0].getFloat() ?: 0f
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // FLOAT / BOOL
                        val arg2: Int = if (arguments[1].getBool() == true) 1 else 0
                        return Value(ColumnType.FLOAT, arg1 / arg2.toFloat())
                    }

                    ColumnType.INT -> {
                        // FLOAT / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.FLOAT, arg1 / arg2.toFloat())
                    }

                    ColumnType.LONG -> {
                        // FLOAT / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.FLOAT, arg1 / arg2.toFloat())
                    }

                    ColumnType.FLOAT -> {
                        // FLOAT / FLOAT
                        val arg2: Float = arguments[1].getFloat() ?: 0.0f
                        return Value(ColumnType.FLOAT, arg1 / arg2)
                    }

                    ColumnType.DOUBLE -> {
                        // FLOAT / DOUBLE
                        val arg2: Double = arguments[1].getDouble() ?: 0.0
                        return Value(ColumnType.DOUBLE, arg1.toDouble() / arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.DOUBLE -> {
                val arg1: Double = arguments[0].getDouble() ?: 0.0
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // DOUBLE / BOOL
                        val arg2: Int = if (arguments[1].getBool() == true) 1 else 0
                        return Value(ColumnType.DOUBLE, arg1 / arg2.toDouble())
                    }

                    ColumnType.INT -> {
                        // DOUBLE / INT
                        val arg2: Int = arguments[1].getInt() ?: 0
                        return Value(ColumnType.DOUBLE, arg1 / arg2.toDouble())
                    }

                    ColumnType.LONG -> {
                        // DOUBLE / LONG
                        val arg2: Long = arguments[1].getLong() ?: 0
                        return Value(ColumnType.DOUBLE, arg1 / arg2.toDouble())
                    }

                    ColumnType.FLOAT -> {
                        // DOUBLE / FLOAT
                        val arg2: Float = arguments[1].getFloat() ?: 0.0f
                        return Value(ColumnType.DOUBLE, arg1 / arg2.toDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // DOUBLE / DOUBLE
                        val arg2: Double = arguments[1].getDouble() ?: 0.0
                        return Value(ColumnType.DOUBLE, arg1 / arg2)
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            else -> throw TypeMismatch("TODO")
        }
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return when (argumentTypes[0]) {
            ColumnType.BOOL -> {
                when(argumentTypes[1]) {
                    ColumnType.BOOL -> ColumnType.INT
                    ColumnType.INT -> ColumnType.INT
                    ColumnType.LONG -> ColumnType.LONG
                    ColumnType.FLOAT -> ColumnType.FLOAT
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.INT -> {
                when(argumentTypes[1]) {
                    ColumnType.BOOL -> ColumnType.INT
                    ColumnType.INT -> ColumnType.INT
                    ColumnType.LONG -> ColumnType.LONG
                    ColumnType.FLOAT -> ColumnType.FLOAT
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.LONG -> {
                when(argumentTypes[1]) {
                    ColumnType.BOOL -> ColumnType.LONG
                    ColumnType.INT -> ColumnType.LONG
                    ColumnType.LONG -> ColumnType.LONG
                    ColumnType.FLOAT -> ColumnType.FLOAT
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.FLOAT -> {
                when(argumentTypes[1]) {
                    ColumnType.BOOL -> ColumnType.FLOAT
                    ColumnType.INT -> ColumnType.FLOAT
                    ColumnType.LONG -> ColumnType.FLOAT
                    ColumnType.FLOAT -> ColumnType.FLOAT
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.DOUBLE -> {
                when(argumentTypes[1]) {
                    ColumnType.BOOL -> ColumnType.DOUBLE
                    ColumnType.INT -> ColumnType.DOUBLE
                    ColumnType.LONG -> ColumnType.DOUBLE
                    ColumnType.FLOAT -> ColumnType.DOUBLE
                    ColumnType.DOUBLE -> ColumnType.DOUBLE
                    else -> throw TypeMismatch("TODO")
                }
            }
            ColumnType.STRING -> {
                when(argumentTypes[1]) {
                    ColumnType.STRING -> ColumnType.STRING
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
