package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.*

class TopTest {
    @Test
    fun testTop() {
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
        val tds = Top(GetColumnByIndex(0), false, 3).apply { initialize(ds.getColumns()) }.transform(ds)
        runBlocking {
            val r = tds.fetchAll().await()
            assertEquals(r.size, 3)
            assertEquals(r[0].getColumn(0).getInt(), 50)
            assertEquals(r[1].getColumn(0).getInt(), 40)
            assertEquals(r[2].getColumn(0).getInt(), 30)
        }
    }
}