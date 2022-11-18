package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import kotlin.math.abs

class UnaryMathFunction<F : (Double) -> Double?>(private val f: F) : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DOUBLE
    }

    override fun call(arguments: List<Value>): Value {
        return Value(ColumnType.DOUBLE, f(arguments[0].getDouble() ?: return Value.NULL))
    }
}

class BinaryMathFunction<F : (Double, Double) -> Double?>(private val f: F) : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DOUBLE
    }

    override fun call(arguments: List<Value>): Value {
        return Value(
            ColumnType.DOUBLE,
            f(arguments[0].getDouble() ?: return Value.NULL, arguments[1].getDouble() ?: return Value.NULL)
        )
    }
}

/// Abs is a special math function, it returns input type
class AbsFunction : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return argumentTypes[0]
    }

    override fun call(arguments: List<Value>): Value {
        when (arguments[0].getValueType()) {
            ColumnType.INT -> return Value(ColumnType.INT, abs(arguments[0].getInt() ?: return Value.NULL))
            ColumnType.LONG -> return Value(ColumnType.LONG, abs(arguments[0].getLong() ?: return Value.NULL))
            ColumnType.FLOAT -> return Value(ColumnType.FLOAT, abs(arguments[0].getFloat() ?: return Value.NULL))
            ColumnType.DOUBLE -> return Value(ColumnType.DOUBLE, abs(arguments[0].getDouble() ?: return Value.NULL))
            else -> return Value.NULL
        }
    }
}