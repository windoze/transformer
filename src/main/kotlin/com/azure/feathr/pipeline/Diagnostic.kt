package com.azure.feathr.pipeline

import kotlinx.coroutines.future.await

suspend fun DataSet.dump(count: Int = 0) {
    val header = getColumns().dump()
    println(header)
    println("-".repeat(header.length))
    (if (count == 0) fetchAll() else fetch(count)).await().forEach {
        println(it.dump())
    }
}

fun Row.dump(): String {
    return "[" + List(getColumns().size) { idx ->
        getColumn(idx).dump()
    }.joinToString(", ") + "]"
}

fun Value.dump(): String {
    return "${getValueType()}(${getDynamic().toString()})"
}

fun List<Column>.dump(): String {
    return joinToString(", ") {
        it.toString()
    }
}

fun List<Row>.dump() {
    forEach {
        println(it.dump())
    }
}