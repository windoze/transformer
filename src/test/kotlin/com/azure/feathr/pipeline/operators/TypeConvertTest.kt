package com.azure.feathr.pipeline.operators

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class TypeConvertTest {
    @Test
    fun testConv() {
        assertEquals(
            FunctionCall("to_bool").apply{ initialize(listOf()) }.apply(listOf(Value(ColumnType.BOOL, true))).getBool(),
            true
        )
        assertEquals(
            FunctionCall("to_int").apply{ initialize(listOf()) }.apply(listOf(Value(ColumnType.BOOL, true))).getInt(),
            1
        )
        assertEquals(
            FunctionCall("to_int").apply{ initialize(listOf()) }.apply(listOf(Value(ColumnType.STRING, "42"))).getInt(),
            42
        )
        assertEquals(
            FunctionCall("to_double").apply{ initialize(listOf()) }.apply(listOf(Value(ColumnType.STRING, "42"))).getDouble(),
            42.toDouble()
        )
        assertEquals(
            FunctionCall("to_string").apply{ initialize(listOf()) }.apply(listOf(Value(ColumnType.INT, 42))).getString(),
            "42"
        )
    }
}