package com.azure.feathr.pipeline

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.operators.GreaterThan
import com.azure.feathr.pipeline.transformations.ProjectRemove
import com.azure.feathr.pipeline.transformations.Top
import com.azure.feathr.pipeline.transformations.Where

class PipelineTest {
    @Test
    fun testPipeline() {
        val ds = EagerDataSet(
            listOf(
                Column("intField1", ColumnType.INT),
                Column("intField2", ColumnType.INT),
            ),
            listOf(
                listOf(10, 100),
                listOf(20, 200),
                listOf(30, 300),
                listOf(40, 400),
                listOf(50, 500),
            )
        )

        val t1 = Where(OperatorExpression(GreaterThan(), listOf(
            GetColumn("intField1"),
            ConstantExpression(20, ColumnType.INT)
        )))

        val t2 = ProjectRemove(listOf("intField1"))

        val t3 = Top(GetColumn("intField2"), false, 2)

        val p = Pipeline(ds.getColumns(), listOf(t1, t2, t3))

        assertEquals(1, p.outputSchema.size)
        assertEquals(ColumnType.INT, p.outputSchema[0].type)
        assertEquals("intField2", p.outputSchema[0].name)

        val tds = runBlocking{ p.process(ds).fetchAll().await() }
        assertEquals(2, tds.size)
        assertEquals(500, tds[0].getColumn("intField2").getInt())
        assertEquals(400, tds[1].getColumn("intField2").getInt())
    }
}