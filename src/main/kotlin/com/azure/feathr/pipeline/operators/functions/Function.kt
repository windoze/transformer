package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import kotlin.math.*

interface Function {
    fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }

    fun call(arguments: List<Value>): Value

    companion object {
        val functions: MutableMap<String, Function> = mutableMapOf()

        init {
            // Predefined function

            // Type conversion
            Function.register("to_bool", TypeConvertor(ColumnType.BOOL))
            Function.register("to_int", TypeConvertor(ColumnType.INT))
            Function.register("to_long", TypeConvertor(ColumnType.LONG))
            Function.register("to_float", TypeConvertor(ColumnType.FLOAT))
            Function.register("to_double", TypeConvertor(ColumnType.DOUBLE))
            Function.register("to_string", TypeConvertor(ColumnType.STRING))
            Function.register("to_array", TypeConvertor(ColumnType.ARRAY))
            Function.register("to_object", TypeConvertor(ColumnType.OBJECT))
            Function.register("to_dynamic", TypeConvertor(ColumnType.DYNAMIC))

            // String ops
            Function.register("substring", Substring())
            Function.register("split", Split())

            // Array ops
            Function.register("make_array", MakeArray())

            // Misc
            Function.register("len", Len())
            Function.register("case", Case())
            Function.register("bucket", Bucket())
            Function.register("timestamp", Timestamp())

            // Math
            Function.register("abs", AbsFunction())
            Function.register("sqrt", UnaryMathFunction(Math::sqrt))
            Function.register("exp", UnaryMathFunction(Math::exp))
            Function.register("log", BinaryMathFunction(::log))
            Function.register("ln", UnaryMathFunction(Math::log))
            Function.register("log10", UnaryMathFunction(Math::log10))
            Function.register("log2", UnaryMathFunction { x -> log(x, 2.toDouble()) })
            Function.register("ceil", UnaryMathFunction(Math::ceil))
            Function.register("floor", UnaryMathFunction(Math::floor))
            Function.register("round", UnaryMathFunction(::round))
            Function.register("sin", UnaryMathFunction(Math::sin))
            Function.register("cos", UnaryMathFunction(Math::cos))
            Function.register("tan", UnaryMathFunction(Math::tan))
            Function.register("asin", UnaryMathFunction(Math::asin))
            Function.register("acos", UnaryMathFunction(Math::acos))
            Function.register("atan", UnaryMathFunction(Math::atan))
            Function.register("sinh", UnaryMathFunction(Math::sinh))
            Function.register("cosh", UnaryMathFunction(Math::cosh))
            Function.register("tanh", UnaryMathFunction(Math::tanh))
            Function.register("asinh", UnaryMathFunction(::asinh))
            Function.register("acosh", UnaryMathFunction(::acosh))
            Function.register("atanh", UnaryMathFunction(::atanh))
        }

        @JvmStatic
        fun register(name: String, function: Function) {
            functions[name] = function
        }
    }
}

