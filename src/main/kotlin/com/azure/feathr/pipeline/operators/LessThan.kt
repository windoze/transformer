package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.TypeMismatch
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class LessThan : Operator {
    override val arity: Int = 2 // `-1` means various length, some functions need it
    override fun apply(arguments: List<Value>): Value {
        when (arguments[0].getValueType()) {
            ColumnType.BOOL -> {
                when (arguments[1].getValueType()) {
                    ColumnType.BOOL -> {
                        // BOOL > BOOL
                        return Value(arguments[0].getBool() <  arguments[1].getBool())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.INT, ColumnType.LONG -> {
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // INT > INT
                        return Value(arguments[0].getLong() <  arguments[1].getLong())
                    }

                    ColumnType.LONG -> {
                        // INT > LONG
                        return Value(arguments[0].getLong() <  arguments[1].getLong())
                    }

                    ColumnType.FLOAT -> {
                        // INT > FLOAT
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // INT > DOUBLE
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.FLOAT, ColumnType.DOUBLE -> {
                when (arguments[1].getValueType()) {
                    ColumnType.INT -> {
                        // FLOAT > INT
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    ColumnType.LONG -> {
                        // FLOAT > LONG
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    ColumnType.FLOAT -> {
                        // FLOAT > FLOAT
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    ColumnType.DOUBLE -> {
                        // FLOAT > DOUBLE
                        return Value(arguments[0].getDouble() <  arguments[1].getDouble())
                    }

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.STRING -> {
                when (arguments[1].getValueType()) {
                    ColumnType.STRING -> return Value(arguments[0].getString() <  arguments[1].getString())
                    else -> throw TypeMismatch("TODO")
                }
            }

            else -> throw TypeMismatch("TODO")
        }
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.BOOL
    }

    override fun dump(arguments: List<String>): String {
        return "(${arguments[0]} < ${arguments[1]})"
    }
}
