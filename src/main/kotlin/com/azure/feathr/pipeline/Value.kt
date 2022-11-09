package com.azure.feathr.pipeline

/**
 * Value class
 */
data class Value(val type: ColumnType, val value: Any?) {
    fun getValueType(): ColumnType {
        return type
    }

    fun getBool(): Boolean? {
        return value as? Boolean
    }

    fun getInt(): Int? {
        if (value is Long) return value.toInt()
        if (value is Float) return value.toInt()
        if (value is Double) return value.toInt()
        return value as? Int
    }

    fun getLong(): Long? {
        if (value is Int) return value.toLong()
        if (value is Float) return value.toLong()
        if (value is Double) return value.toLong()
        return value as? Long
    }

    fun getFloat(): Float? {
        if (value is Int) return value.toFloat()
        if (value is Long) return value.toFloat()
        if (value is Double) return value.toFloat()
        return value as? Float
    }

    fun getDouble(): Double? {
        if (value is Int) return value.toDouble()
        if (value is Long) return value.toDouble()
        if (value is Float) return value.toDouble()
        return value as? Double
    }

    fun getString(): String? {
        return value as? String
    }

    fun getArray(): List<Any?>? {
        return value as? List<Any?>
    }

    @Suppress("UNCHECKED_CAST")
    fun getObject(): Map<String, Any?>? {
        return value as? Map<String, Any?>
    }

    fun getDynamic(): Any? {
        return value
    }

    fun toStatic(): Value {
        return if (type != ColumnType.DYNAMIC) {
            this
        } else {
            when (value) {
                null -> Value(ColumnType.INT, null)
                is Boolean -> Value(ColumnType.BOOL, value)
                is Int -> Value(ColumnType.INT, value)
                is Long -> Value(ColumnType.LONG, value)
                is Float -> Value(ColumnType.FLOAT, value)
                is Double -> Value(ColumnType.DOUBLE, value)
                is String -> Value(ColumnType.STRING, value)
                is List<*> -> Value(ColumnType.ARRAY, value)
                is Map<*, *> -> Value(ColumnType.OBJECT, value)
                else -> throw TypeMismatch("TODO")
            }
        }
    }

    companion object {
        val NULL = Value(ColumnType.DYNAMIC, null)
    }
}
