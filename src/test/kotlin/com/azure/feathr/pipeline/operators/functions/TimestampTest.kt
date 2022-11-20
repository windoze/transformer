package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TimestampTest {
    @Test
    fun testParse() {
        // 2020-4-1 00:44:02 UTC
        val tsUtc = Timestamp().call(listOf(
            Value("2020-04-01 00:44:02"),
            Value("%Y-%m-%d %H:%M:%S"),
        ))
        assertEquals(1585701842, tsUtc.getLong())

        // Same time, but in GMT+8
        val tsCst = Timestamp().call(listOf(
            Value("2020-04-01 08:44:02"),
            Value("%Y-%m-%d %H:%M:%S"),
            Value("Asia/Shanghai"),
        ))
        assertEquals(1585701842, tsCst.getLong())
    }
}