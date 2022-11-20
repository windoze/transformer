package com.azure.feathr.pipeline

/**
 * Value class
 */
data class Value(val value: Any?) {
    init {
        // Validate if the value is one of the expected types
        getValueType()
    }

    fun getValueType(): ColumnType {
        if(value == null) return ColumnType.NULL
        return when(value) {
            is Boolean -> ColumnType.BOOL
            is Int -> ColumnType.INT
            is Long -> ColumnType.LONG
            is Float -> ColumnType.FLOAT
            is Double -> ColumnType.DOUBLE
            is String -> ColumnType.STRING
            is List<*> -> ColumnType.ARRAY
            is Map<*, *> -> ColumnType.OBJECT
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

    fun getError(): TransformerException {
        return value as? TransformerException ?: throw IllegalValue(value)
    }

    @Suppress("UNCHECKED_CAST")
    fun getObject(): Map<String, Any?> {
        return value as? Map<String, Any?> ?: throw IllegalValue(value)
    }

    companion object {
        val NULL = Value(null)
    }
}
