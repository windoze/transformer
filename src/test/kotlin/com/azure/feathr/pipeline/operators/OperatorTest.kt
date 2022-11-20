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
                    Value(10),
                    Value(32),
                )
            ).getInt(), 42
        )

        assertEquals(
            Minus().apply(
                listOf(
                    Value(72),
                    Value(30.toLong()),
                )
            ).getLong(), 42L
        )

        assertEquals(
            Multiply().apply(
                listOf(
                    Value(7.toFloat()),
                    Value(6.toLong()),
                )
            ).getFloat(), 42.toFloat()
        )

        assertEquals(
            Divide().apply(
                listOf(
                    Value(84),
                    Value(2.toDouble()),
                )
            ).getDouble(), 42.toDouble()
        )
    }

    @Test
    fun testTypeConv() {
        assertEquals(
            Plus().apply(
                listOf(
                    Value(10),
                    Value(32),
                )
            ).getValueType(), ColumnType.LONG
        )
    }
}