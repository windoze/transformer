package com.azure.feathr.pipeline

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DataSetTest {
    @Test
    fun testFetch() {
        val ds = EagerDataSet(
            listOf(
                Column("intField1", ColumnType.INT),
                Column("intField2", ColumnType.INT),
                Column("strField", ColumnType.STRING)
            ),
            listOf(
                listOf(10, 100, "foo1"),
                listOf(20, 200, "foo2"),
                listOf(30, 300, "foo3"),
                listOf(40, 400, "foo4"),
                listOf(50, 500, "foo5"),
            )
        )

        val r = runBlocking { ds.fetch(3).await() }
        assertEquals(r.size, 3)
        // Now only 2 rows left
        val rr = runBlocking { ds.fetch(3).await() }
        assertEquals(rr.size, 2)
        rr.forEach {
            println(runBlocking { it.dump() })
        }
    }
}