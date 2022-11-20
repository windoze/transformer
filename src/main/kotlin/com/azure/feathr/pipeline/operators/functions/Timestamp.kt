package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TransformerException
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
        // TODO: Convert exception type
        try {
            val str = arguments[0].getString()
            val formatter = arguments.getOrNull(1)?.getString()
            val zone = arguments.getOrNull(2)?.getString()?.let { ZoneId.of(it) } ?: UTC
            val dt = if (formatter == null)
                LocalDateTime.parse(str)
            else {
                LocalDateTime.parse(str, StrftimeFormatter.toDateTimeFormatter(formatter))
            }.atZone(zone).toInstant()

            val timestamp =
                Timestamp.from(dt)
            return Value(timestamp.time.toDouble() / 1000)
        } catch (e: TransformerException) {
            return Value(e)
        }
    }

    companion object {
        val UTC: ZoneId = ZoneId.of("UTC")
    }
}