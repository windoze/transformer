package com.azure.feathr.pipeline.operators

import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import com.azure.feathr.pipeline.operators.functions.*
import com.azure.feathr.pipeline.operators.functions.Function

class FunctionCall(private val name: String) : Operator {
    override val arity: Int = -1

    private var function: Function? = null

    override fun apply(arguments: List<Value>): Value {
        return function?.call(arguments) ?: Value(ColumnType.DYNAMIC, null)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return function?.getResultType(argumentTypes) ?: ColumnType.DYNAMIC
    }

    override fun initialize(columns: List<Column>) {
        function = Function.functions[name]
    }

    override fun dump(arguments: List<String>): String {
        return "$name(${arguments.joinToString(", ")})"
    }

    companion object {
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
        }
    }
}

