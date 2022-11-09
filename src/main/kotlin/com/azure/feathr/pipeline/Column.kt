package com.azure.feathr.pipeline

data class Column(val name: String, val type: ColumnType) {
    override fun toString(): String {
        return "$name:$type"
    }
}