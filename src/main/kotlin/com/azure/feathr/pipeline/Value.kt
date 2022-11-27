package com.azure.feathr.pipeline

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import java.time.*
import java.time.format.DateTimeFormatter


/**
 * Value class
 */
class Value(v: Any?) {
    val value: Any?

    init {
        value = when(v) {
            null -> null
            is Boolean -> v
            is Int -> v
            is Long -> v
            is Float -> v
            is Double -> v
            is String -> v
            is List<*> -> v
            is Map<*, *> -> v
            is OffsetDateTime -> v
            is TransformerException -> v
            is Value -> v.value
            is Throwable -> TransformExternalException(v.toString())
            else -> throw IllegalValue(v)
        }
    }

    fun getValueType(): ColumnType {
        if (value == null) return ColumnType.NULL
        return when (value) {
            is Boolean -> ColumnType.BOOL
            is Int -> ColumnType.INT
            is Long -> ColumnType.LONG
            is Float -> ColumnType.FLOAT
            is Double -> ColumnType.DOUBLE
            is String -> ColumnType.STRING
            is List<*> -> ColumnType.ARRAY
            is Map<*, *> -> ColumnType.OBJECT
            is OffsetDateTime -> ColumnType.DATETIME
            is TransformerException -> ColumnType.ERROR
            else -> throw IllegalValue(value)
        }
    }

    fun isNull(): Boolean {
        return value == null
    }

    fun isError(): Boolean {
        return value is TransformerException
    }

    fun getBool(): Boolean {
        return value as? Boolean ?: throw IllegalValue(value)
    }

    fun getInt(): Int {
        if (value is Long) return value.toInt()
        if (value is Float) return value.toInt()
        if (value is Double) return value.toInt()
        return value as? Int ?: throw IllegalValue(value)
    }

    fun getLong(): Long {
        if (value is Int) return value.toLong()
        if (value is Float) return value.toLong()
        if (value is Double) return value.toLong()
        return value as? Long ?: throw IllegalValue(value)
    }

    fun getFloat(): Float {
        if (value is Int) return value.toFloat()
        if (value is Long) return value.toFloat()
        if (value is Double) return value.toFloat()
        return value as? Float ?: throw IllegalValue(value)
    }

    fun getDouble(): Double {
        if (value is Int) return value.toDouble()
        if (value is Long) return value.toDouble()
        if (value is Float) return value.toDouble()
        return value as? Double ?: throw IllegalValue(value)
    }

    fun getString(): String {
        return value as? String ?: throw IllegalValue(value)
    }

    fun getArray(): List<Any?> {
        return value as? List<Any?> ?: throw IllegalValue(value)
    }

    fun getDateTime(): OffsetDateTime {
        if (value is String) return parseDateTime(value)
        return value as? OffsetDateTime ?: throw IllegalValue(value)
    }

    fun getError(): TransformerException {
        return value as? TransformerException ?: throw IllegalValue(value)
    }

    @Suppress("UNCHECKED_CAST")
    fun getObject(): Map<String, Any?> {
        return value as? Map<String, Any?> ?: throw IllegalValue(value)
    }

    fun into(cls: Class<*>): Any? {
        if (value == null)
            return null
        when (cls) {
            Int::class.java -> return getInt()
            Long::class.java -> return getLong()
            Float::class.java -> return getFloat()
            Double::class.java -> return getDouble()
            String::class.java -> return getString()
            List::class.java -> return getArray()
            Map::class.java -> return getObject()
            OffsetDateTime::class.java -> return getDateTime()
            TransformerException::class.java -> return getError()
        }
        throw IllegalType(ColumnType.NULL)  // TODO:
    }

    companion object {
        val NULL = Value(null)

        val DEFAULT_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"))!!
        val DEFAULT_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"))!!
        val DEFAULT_DATETIME_FORMAT_PARSER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.[SSSSSSSSS][SSSSSSSS][SSSSSSS][SSSSSS][SSSSS][SSSS][SSS][SS][S]]").withZone(ZoneId.of("UTC"))!!

        @JvmStatic
        fun parseDateTime(s: String): OffsetDateTime {
            try {
                return LocalDateTime.parse(s, DEFAULT_DATETIME_FORMAT_PARSER).atOffset(ZoneOffset.UTC)
            } catch (e: DateTimeException) {
                try {
                    return LocalDate.parse(s, DEFAULT_DATE_FORMAT).atTime(0, 0, 0).atOffset(ZoneOffset.UTC)
                } catch (e: DateTimeException) {
                    throw IllegalValue(s)
                }
            }
        }

        @JvmStatic
        val jacksonModule = SimpleModule().addSerializer(OffsetDateTime::class.java, OffsetDateTimeSerializer())

        class OffsetDateTimeSerializer : JsonSerializer<OffsetDateTime?>() {
            override fun serialize(value: OffsetDateTime?, gen: JsonGenerator, serializers: SerializerProvider) {
                gen.writeString(value?.format(DEFAULT_DATETIME_FORMAT) ?: "")
            }
        }
    }
}
