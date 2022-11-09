package com.azure.feathr.pipeline.transformations

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ProjectRenameTest {
    @Test
    fun testProjectRename() {
        val ds = com.azure.feathr.pipeline.EagerDataSet(
            listOf(
                com.azure.feathr.pipeline.Column("intField1", com.azure.feathr.pipeline.ColumnType.INT),
                com.azure.feathr.pipeline.Column("intField2", com.azure.feathr.pipeline.ColumnType.INT),
                com.azure.feathr.pipeline.Column("strField", com.azure.feathr.pipeline.ColumnType.STRING)
            ),
            listOf(
                listOf(10, 100, "foo1"),
                listOf(20, 200, "foo2"),
                listOf(30, 300, "foo3"),
                listOf(40, 400, "foo4"),
                listOf(50, 500, "foo5"),
            )
        )

        val tds = ProjectRename(mapOf("intField1" to "intField1Renamed", "strField" to "strFieldRenamed")).apply { initialize(ds.getColumns()) }.transform(ds)
        assertEquals("intField1Renamed", tds.getColumns()[0].name)
        assertEquals("intField2", tds.getColumns()[1].name)
        assertEquals("strFieldRenamed", tds.getColumns()[2].name)
    }
}