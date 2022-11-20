package com.azure.feathr.pipeline

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ValueTest {
    @Test
    fun testToStatic() {
        run {
            val v = Value(42)
            assertEquals(v.getValueType(), ColumnType.INT)
            assertEquals(v.getInt(), 42)
        }
        run {
            val v = Value("foo")
            assertEquals(v.getValueType(), ColumnType.STRING)
            assertEquals(v.value, "foo")
        }
    }
}