package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.TransformerException
import com.azure.feathr.pipeline.Value
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class Timestamp : Function {

    override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DATETIME
    }

    override fun call(arguments: List<Value>): Value {
        // TODO: Convert exception type
        return try {
            val str = arguments[0].getString()
            val dt = toDateTime(str, arguments.getOrNull(1)?.getString() ?:"", arguments.getOrNull(2)?.getString() ?:"")

            Value(dt)
        } catch (e: TransformerException) {
            Value(e)
        }
    }

    companion object {
    }
}

fun toDateTime(s: String, formatter: String = "", zone: String = "UTC"): OffsetDateTime {
    val tz = ZoneId.of(zone)

    return if (formatter.isBlank()) {
        Value.parseDateTime(s)
    } else {
        LocalDateTime.parse(s, StrftimeFormatter.toDateTimeFormatter(formatter)).atZone(tz).toOffsetDateTime()
    }
}