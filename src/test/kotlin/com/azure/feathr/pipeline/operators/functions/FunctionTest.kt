package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class FunctionTest {
    @Test
    fun testWrapper() {
        val cos = Function.unary(Math::cos)
        assertEquals(Value(1.0), cos.call(listOf(Value(0))))

        println("${cos.getResultType(listOf(ColumnType.DOUBLE))}")
    }
}