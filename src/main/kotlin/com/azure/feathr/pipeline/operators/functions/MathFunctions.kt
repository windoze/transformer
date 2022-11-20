package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import kotlin.math.abs

class UnaryMathFunction<F : (Double) -> Double?>(private val f: F) : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DOUBLE
    }

    override fun call(arguments: List<Value>): Value {
        return Value(f(arguments[0].getDouble()))
    }
}

class BinaryMathFunction<F : (Double, Double) -> Double?>(private val f: F) : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DOUBLE
    }

    override fun call(arguments: List<Value>): Value {
        return Value(
            f(arguments[0].getDouble(), arguments[1].getDouble())
        )
    }
}

/// Abs is a special math function, it returns input type
class AbsFunction : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return argumentTypes[0]
    }

    override fun call(arguments: List<Value>): Value {
        return when (arguments[0].getValueType()) {
            ColumnType.INT -> Value(abs(arguments[0].getInt()))
            ColumnType.LONG -> Value(abs(arguments[0].getLong()))
            ColumnType.FLOAT -> Value(abs(arguments[0].getFloat()))
            ColumnType.DOUBLE -> Value(abs(arguments[0].getDouble()))
            else -> Value.NULL
        }
    }
}