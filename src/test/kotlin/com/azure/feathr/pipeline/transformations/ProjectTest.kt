package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.*
import com.azure.feathr.pipeline.operators.Plus

class ProjectTest {
    @Test
    fun testProject() {
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

        val t = Project(
            listOf(
                Pair(
                    "longField1", OperatorExpression(
                        Plus(), listOf(
                            GetColumnByIndex(1),
                            ConstantExpression(10.toLong())
                        )
                    )
                ),
                Pair(
                    "intField3", OperatorExpression(
                        Plus(), listOf(
                            GetColumnByIndex(0),
                            GetColumnByIndex(1),
                        )
                    )
                )
            )
        ).apply { initialize(ds.getColumns()) }
        val tds = t.transform(ds)
        runBlocking { tds.dump() }
    }
}