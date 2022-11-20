package com.azure.feathr.pipeline

import com.azure.feathr.pipeline.operators.Plus
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ExpressionTest {
    @Test
    fun testEvalConst() {
        val c = ConstantExpression(10)
        val r = EagerRow(listOf(), listOf())
        assertEquals(c.evaluate(r).getInt(), 10)
    }

    @Test
    fun testGetColumn() {
        val r = EagerRow(
            listOf(
                Column("intField", ColumnType.INT),
                Column("strField", ColumnType.STRING)
            ),
            listOf(10, "foo")
        )
        assertEquals(GetColumnByIndex(0).evaluate(r).getInt(), 10)
        assertEquals(GetColumnByIndex(1).evaluate(r).getString(), "foo")
    }

    @Test
    fun testPlusExp() {
        val r = EagerRow(
            listOf(
                Column("intField1", ColumnType.INT),
                Column("intField2", ColumnType.INT),
                Column("strField", ColumnType.STRING)
            ),
            listOf(10, 32, "foo")
        )
        assertEquals(
            OperatorExpression(
                Plus(), listOf(GetColumnByIndex(0), GetColumnByIndex(1))
            ).evaluate(r).getInt(), 42
        )
    }
}