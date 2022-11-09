package com.azure.feathr.pipeline

enum class ColumnType {
    BOOL,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    ARRAY,
    OBJECT,
    DYNAMIC;

    fun validate(v: Any?): Any? {
        if (v == null) return v
        return when (v) {
            is Boolean -> v
            is Int -> v
            is Long -> v
            is Float -> v
            is Double -> v
            is String -> v
            is List<*> -> v.map { validate(it) }.toList()
            is Map<*, *> -> v.map { (key, value) ->
                key.apply {
                    if (key !is String) throw IllegalValue(v)
                } to validate(value)
            }.toMap()

            else -> throw IllegalValue(v)
        }
    }

    fun coerce(v: Any?): Any? {
        if (v == null) return null
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

            STRING -> if (v is String) v else throw IllegalValue(v)
            ARRAY -> if (v is List<*>) validate(v) else throw IllegalValue(v)
            OBJECT -> if (v is Map<*, *>) validate(v) else throw IllegalValue(v)
            DYNAMIC -> validate(v)
        }
    }

    fun checkedConvert(v: Any?): Any? {
        if (v == null) return null
        return when (this) {
            BOOL -> when (v) {
                is Boolean -> v
                is Int -> (v != 0)
                is Long -> (v != 0L)
                is Float -> (v != 0.0f)
                is Double -> (v != 0.0)
                is String -> v.isNotEmpty()
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

            DYNAMIC -> v
        }
    }

    fun convertTo(v: Any?): Any? {
        if (v == null) return null
        return when (this) {
            BOOL -> when (v) {
                is Boolean -> v
                is Int -> (v != 0)
                is Long -> (v != 0L)
                is Float -> (v != 0.0f)
                is Double -> (v != 0.0)
                is String -> v.isNotEmpty()
                is List<*> -> v.isNotEmpty()
                is Map<*, *> -> v.isNotEmpty()
                else -> null
            }

            INT -> when (v) {
                is Boolean -> (if (v) 1 else 0).toInt()
                is Int -> v
                is Long -> v.toInt()
                is Float -> v.toInt()
                is Double -> v.toInt()
                is String -> v.toInt()
                else -> null
            }

            LONG -> when (v) {
                is Boolean -> (if (v) 1 else 0).toLong()
                is Int -> v.toLong()
                is Long -> v.toLong()
                is Float -> v.toLong()
                is Double -> v.toLong()
                is String -> v.toLong()
                else -> null
            }

            FLOAT -> when (v) {
                is Boolean -> (if (v) 1 else 0).toFloat()
                is Int -> v.toFloat()
                is Long -> v.toFloat()
                is Float -> v.toFloat()
                is Double -> v.toFloat()
                is String -> v.toFloat()
                else -> null
            }

            DOUBLE -> when (v) {
                is Boolean -> (if (v) 1 else 0).toDouble()
                is Int -> v.toDouble()
                is Long -> v.toDouble()
                is Float -> v.toDouble()
                is Double -> v.toDouble()
                is String -> v.toDouble()
                else -> null
            }

            STRING -> when (v) {
                is Boolean -> (if (v) "true" else "false")
                is Int -> v.toString()
                is Long -> v.toString()
                is Float -> v.toString()
                is Double -> v.toString()
                is String -> v
                else -> null
            }

            ARRAY -> when (v) {
                is List<*> -> v
                else -> null
            }

            OBJECT -> when (v) {
                is Map<*, *> -> v
                else -> null
            }

            DYNAMIC -> v
        }
    }
}