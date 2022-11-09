package com.azure.feathr.pipeline

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ValueTest {
    @Test
    fun testToStatic() {
        run {
            val dv = Value(ColumnType.DYNAMIC, 42)
            val sv = dv.toStatic()
            assertEquals(sv.getValueType(), ColumnType.INT)
            assertEquals(sv.getDynamic(), 42)
        }
        run {
            val dv = Value(ColumnType.DYNAMIC, "foo")
            val sv = dv.toStatic()
            assertEquals(sv.getValueType(), ColumnType.STRING)
            assertEquals(sv.getDynamic(), "foo")
        }
    }
}