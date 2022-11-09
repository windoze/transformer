package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TypeMismatch
import com.azure.feathr.pipeline.Value

class TypeConvertor(private val type: ColumnType): Function {
    override fun call(arguments: List<Value>): Value {
        val argument = arguments[0]
        val argType = argument.getValueType()
        if (argType == type) return argument
        if (type == ColumnType.DYNAMIC) return Value(ColumnType.DYNAMIC, argument.getDynamic())
        if (argType == ColumnType.DYNAMIC) return call(listOf(argument.toStatic()))

        when (argType) {
            ColumnType.BOOL -> {
                val arg = argument.getBool()
                return when (type) {
                    ColumnType.INT -> Value(type, arg?.let {
                        if (it) 1 else 0
                    })

                    ColumnType.LONG -> Value(type, arg?.let {
                        if (it) 1 else 0
                    }?.toLong())

                    ColumnType.FLOAT -> Value(type, arg?.let {
                        if (it) 1 else 0
                    }?.toFloat())

                    ColumnType.DOUBLE -> Value(type, arg?.let {
                        if (it) 1 else 0
                    }?.toDouble())

                    ColumnType.STRING -> Value(type, arg?.let {
                        if (it) "true" else "false"
                    })

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.INT -> {
                val arg = argument.getInt()
                return when (type) {
                    ColumnType.BOOL -> Value(type, arg?.let {
                        it != 0
                    })

                    ColumnType.LONG -> Value(type, arg?.toLong())

                    ColumnType.FLOAT -> Value(type, arg?.toFloat())

                    ColumnType.DOUBLE -> Value(type, arg?.toDouble())

                    ColumnType.STRING -> Value(type, arg?.toString())

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.LONG -> {
                val arg = argument.getLong()
                return when (type) {
                    ColumnType.BOOL -> Value(type, arg?.let {
                        it != 0L
                    })

                    ColumnType.INT -> Value(type, arg?.toInt())

                    ColumnType.FLOAT -> Value(type, arg?.toFloat())

                    ColumnType.DOUBLE -> Value(type, arg?.toDouble())

                    ColumnType.STRING -> Value(type, arg?.toString())

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.FLOAT -> {
                val arg = argument.getFloat()
                return when (type) {
                    ColumnType.BOOL -> Value(type, arg?.let {
                        it != 0.0.toFloat()
                    })

                    ColumnType.INT -> Value(type, arg?.toInt())

                    ColumnType.LONG -> Value(type, arg?.toLong())

                    ColumnType.DOUBLE -> Value(type, arg?.toDouble())

                    ColumnType.STRING -> Value(type, arg?.toString())

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.DOUBLE -> {
                val arg = argument.getDouble()
                return when (type) {
                    ColumnType.BOOL -> Value(type, arg?.let {
                        it != 0.toDouble()
                    })

                    ColumnType.INT -> Value(type, arg?.toInt())

                    ColumnType.LONG -> Value(type, arg?.toLong())

                    ColumnType.FLOAT -> Value(type, arg?.toFloat())

                    ColumnType.STRING -> Value(type, arg?.toString())

                    else -> throw TypeMismatch("TODO")
                }
            }

            ColumnType.STRING -> {
                val arg = argument.getString()
                return when (type) {
                    ColumnType.BOOL -> Value(type, arg?.isNotEmpty())

                    ColumnType.INT -> Value(type, arg?.toInt())

                    ColumnType.LONG -> Value(type, arg?.toLong())

                    ColumnType.FLOAT -> Value(type, arg?.toFloat())

                    ColumnType.DOUBLE -> Value(type, arg?.toDouble())

                    else -> throw TypeMismatch("TODO")
                }
            }

            else -> throw TypeMismatch("TODO")
        }
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return type
    }
}