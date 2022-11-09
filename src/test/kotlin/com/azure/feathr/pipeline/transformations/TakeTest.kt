package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.EagerDataSet

class TakeTest {
    @Test
    fun testTake() {
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

        val tds = Take(3).apply { initialize(ds.getColumns()) }.transform(ds)

        val r = runBlocking { tds.fetchAll().await() }
        assertEquals(r.size, 3)
    }
}