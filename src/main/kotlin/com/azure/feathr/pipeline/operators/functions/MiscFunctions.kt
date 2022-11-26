package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ArityError
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TypeMismatch
import com.azure.feathr.pipeline.Value
import java.time.ZoneId
import kotlin.math.abs


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


class Split : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.ARRAY
    }

    override fun call(arguments: List<Value>): Value {
        val str = arguments[0].getString()
        val sep = arguments[1].getString()
        return Value(str.split(sep))
    }
}

class MakeArray : Function {
    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.ARRAY
    }

    override fun call(arguments: List<Value>): Value {
        return Value(arguments.map { it.value })
    }
}

class Len : Function {
    override fun call(arguments: List<Value>): Value {
        val len = when (val v = arguments[0].value) {
            null -> 0
            is String -> v.length
            is List<*> -> v.size
            is Map<*, *> -> v.size
            else -> throw TypeMismatch("TODO")
        }
        return Value(len)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.INT
    }
}

class Substring : Function {
    override fun call(arguments: List<Value>): Value {
        val s = arguments[0].getString()
        val start = arguments[1].getInt()
        val len = if (arguments.size > 2) arguments[2].getInt() else -1
        val ret = if (len < 0) s.substring(start) else s.substring(start, start + len)
        return Value(ret)
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.STRING
    }
}

class ToUtcTimeStamp : Function {
    override fun call(arguments: List<Value>): Value {
        if (arguments.isEmpty()) throw ArityError("Require 1 or 2 arguments, but 0 provided")
        val dt = arguments[0].getDateTime()
        val zone = if (arguments.size > 1) {
            ZoneId.of(arguments[1].getString())
        } else {
            Function.UTC
        }
        // Same local time, but in different zone, the result is converted into UTC time
        // E.g. ("10AM", "GMT+8) -> 2AM/UTC
        return Value(dt.toLocalDateTime().atZone(zone).withZoneSameInstant(Function.UTC).toOffsetDateTime())
    }

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DATETIME
    }
}