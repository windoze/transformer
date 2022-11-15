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
            Value(ColumnType.STRING, "2020-04-01 00:44:02"),
            Value(ColumnType.STRING, "uuuu-MM-dd kk:mm:ss"),
        ))
        assertEquals(1585701842, tsUtc.getDouble()!!.toLong())

        // Same time, but in GMT+8
        val tsCst = Timestamp().call(listOf(
            Value(ColumnType.STRING, "2020-04-01 08:44:02"),
            Value(ColumnType.STRING, "uuuu-MM-dd kk:mm:ss"),
            Value(ColumnType.STRING, "Asia/Shanghai"),
        ))
        assertEquals(1585701842, tsCst.getDouble()!!.toLong())
    }
}