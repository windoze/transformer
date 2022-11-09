package com.azure.feathr.pipeline.operators

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value

class OperatorTest {
    @Test
    fun testMath() {
        assertEquals(
            Plus().apply(
                listOf(
                    Value(ColumnType.INT, 10),
                    Value(ColumnType.INT, 32),
                )
            ).getInt(), 42
        )

        assertEquals(
            Minus().apply(
                listOf(
                    Value(ColumnType.INT, 72),
                    Value(ColumnType.LONG, 30.toLong()),
                )
            ).getLong(), 42L
        )

        assertEquals(
            Multiply().apply(
                listOf(
                    Value(ColumnType.FLOAT, 7.toFloat()),
                    Value(ColumnType.LONG, 6.toLong()),
                )
            ).getFloat(), 42.toFloat()
        )

        assertEquals(
            Divide().apply(
                listOf(
                    Value(ColumnType.INT, 84),
                    Value(ColumnType.DOUBLE, 2.toDouble()),
                )
            ).getDouble(), 42.toDouble()
        )
    }

    @Test
    fun testTypeConv() {
        assertEquals(
            Plus().apply(
                listOf(
                    Value(ColumnType.INT, 10),
                    Value(ColumnType.LONG, 32),
                )
            ).getValueType(), ColumnType.LONG
        )
    }
}