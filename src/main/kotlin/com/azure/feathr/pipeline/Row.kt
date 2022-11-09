package com.azure.feathr.pipeline

interface Row {
    fun getColumns(): List<Column>

    fun getColumn(index: Int): Value
    fun getColumn(name: String): Value {
        //NOTE: Slow operation, prefer to use index over name
        val idx = getColumns().indexOfFirst {
            it.name == name
        }
        if (idx < 0) throw ColumnNotFound(name)
        return getColumn(idx)
    }

    fun evaluate(): List<Value?> {
        return List(getColumns().size) {
            getColumn(it)
        }
    }
}

// An already computed composited value
class EagerRow(private val columns: List<Column>, val values: List<Any?>) : Row {
    override fun getColumns(): List<Column> {
        return columns
    }

    override fun getColumn(index: Int): Value {
        return Value(columns[index].type, values[index])
    }
}
