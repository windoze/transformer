package com.azure.feathr.pipeline.transformations

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.EagerDataSet

class ProjectRemoveTest {
    @Test
    fun testProjectRemove() {
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

        val tds = ProjectRemove(listOf("intField2")).apply { initialize(ds.getColumns()) }.transform(ds)
        assertEquals(tds.getColumns().size, 2)
        assertEquals(tds.getColumns()[0].name, "intField1")
        assertEquals(tds.getColumns()[1].name, "strField")
    }
}