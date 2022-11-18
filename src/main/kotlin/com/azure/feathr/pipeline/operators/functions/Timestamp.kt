package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class Timestamp : Function {

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DOUBLE
    }

    override fun call(arguments: List<Value>): Value {
        try {
            val str = arguments[0].getString() ?: return Value(ColumnType.DOUBLE, null)
            val formatter = arguments.getOrNull(1)?.getString()
            val zone = arguments.getOrNull(2)?.getString()?.let { ZoneId.of(it) } ?: UTC
            val dt = if (formatter == null)
                LocalDateTime.parse(str)
            else {
//                LocalDateTime.parse(str, DateTimeFormatter.ofPattern(formatter))
                LocalDateTime.parse(str, StrftimeFormatter.toDateTimeFormatter(formatter))
            }.atZone(zone).toInstant()

            val timestamp =
                Timestamp.from(dt)
            return Value(ColumnType.DOUBLE, timestamp.time.toDouble() / 1000)
        } catch (e: Throwable) {
            return Value(ColumnType.DOUBLE, null)
        }
    }

    companion object {
        val UTC: ZoneId = ZoneId.of("UTC")
    }
}