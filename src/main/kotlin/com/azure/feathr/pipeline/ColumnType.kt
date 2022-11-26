package com.azure.feathr.pipeline

import com.azure.feathr.pipeline.Value.Companion.DEFAULT_DATETIME_FORMAT
import java.time.DateTimeException
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

enum class ColumnType {
    NULL, BOOL, INT, LONG, FLOAT, DOUBLE, STRING, ARRAY, OBJECT, DATETIME, DYNAMIC, ERROR;

    fun coerce(v: Any?): Any? {
        if (v == null) return null
        if (v is TransformerException) throw v
        return when (this) {
            BOOL -> if (v is Boolean) v else throw IllegalValue(v)
            INT -> when (v) {
                is Int -> v
                is Long -> v.toInt()
                is Float -> v.toInt()
                is Double -> v.toInt()
                else -> throw IllegalValue(v)
            }

            LONG -> when (v) {
                is Int -> v.toLong()
                is Long -> v
                is Float -> v.toLong()
                is Double -> v.toLong()
                else -> throw IllegalValue(v)
            }

            FLOAT -> when (v) {
                is Int -> v.toFloat()
                is Long -> v.toFloat()
                is Float -> v
                is Double -> v.toFloat()
                else -> throw IllegalValue(v)
            }

            DOUBLE -> when (v) {
                is Int -> v.toDouble()
                is Long -> v.toDouble()
                is Float -> v.toDouble()
                is Double -> v
                else -> throw IllegalValue(v)
            }

            STRING -> when(v) {
                is String -> v
                is OffsetDateTime -> try {
                    v.format(DEFAULT_DATETIME_FORMAT)
                } catch (e: DateTimeException) {
                    throw IllegalValue(v)
                }
                else -> throw IllegalValue(v)
            }
            ARRAY -> if (v is List<*>) v else throw IllegalValue(v)
            OBJECT -> if (v is Map<*, *>) v else throw IllegalValue(v)

            DATETIME -> when(v) {
                is OffsetDateTime -> v
                is String -> try {
                    DEFAULT_DATETIME_FORMAT.parse(v)
                } catch (e: DateTimeParseException) {
                    throw IllegalValue(v)
                }
                else -> throw IllegalValue(v)
            }

            ERROR -> throw IllegalValue(v)
            DYNAMIC -> v
            NULL -> throw IllegalType(this)
        }
    }

    fun convert(v: Any?): Any? {
        if (v == null) return null
        if (v is TransformerException) throw v
        return when (this) {
            BOOL -> when (v) {
                is Boolean -> v
                is Int -> (v != 0)
                is Long -> (v != 0L)
                is Float -> (v != 0.0f)
                is Double -> (v != 0.0)
                is String -> (v == "true")
                is List<*> -> v.isNotEmpty()
                is Map<*, *> -> v.isNotEmpty()
                else -> throw IllegalValue(v)
            }

            INT -> when (v) {
                is Boolean -> (if (v) 1 else 0).toInt()
                is Int -> v
                is Long -> v.toInt()
                is Float -> v.toInt()
                is Double -> v.toInt()
                is String -> v.toInt()
                else -> throw IllegalValue(v)
            }

            LONG -> when (v) {
                is Boolean -> (if (v) 1 else 0).toLong()
                is Int -> v.toLong()
                is Long -> v.toLong()
                is Float -> v.toLong()
                is Double -> v.toLong()
                is String -> v.toLong()
                else -> throw IllegalValue(v)
            }

            FLOAT -> when (v) {
                is Boolean -> (if (v) 1 else 0).toFloat()
                is Int -> v.toFloat()
                is Long -> v.toFloat()
                is Float -> v.toFloat()
                is Double -> v.toFloat()
                is String -> v.toFloat()
                else -> throw IllegalValue(v)
            }

            DOUBLE -> when (v) {
                is Boolean -> (if (v) 1 else 0).toDouble()
                is Int -> v.toDouble()
                is Long -> v.toDouble()
                is Float -> v.toDouble()
                is Double -> v.toDouble()
                is String -> v.toDouble()
                else -> throw IllegalValue(v)
            }

            STRING -> when (v) {
                is Boolean -> (if (v) "true" else "false")
                is Int -> v.toString()
                is Long -> v.toString()
                is Float -> v.toString()
                is Double -> v.toString()
                is String -> v
                is OffsetDateTime -> try {
                    v.format(DEFAULT_DATETIME_FORMAT)
                } catch (e: DateTimeException) {
                    throw IllegalValue(v)
                }
                else -> throw IllegalValue(v)
            }

            ARRAY -> when (v) {
                is List<*> -> v
                else -> throw IllegalValue(v)
            }

            OBJECT -> when (v) {
                is Map<*, *> -> v
                else -> throw IllegalValue(v)
            }

            DATETIME -> when(v) {
                is OffsetDateTime -> v
                is String -> try {
                    Value.parseDateTime(v)
                } catch (e: DateTimeParseException) {
                    throw IllegalValue(v)
                }
                else -> throw IllegalValue(v)
            }

            ERROR -> throw IllegalValue(v)

            DYNAMIC -> v

            NULL -> throw IllegalValue(v)
        }
    }
}