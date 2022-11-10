package com.azure.feathr.pipeline.transformations

import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.azure.feathr.pipeline.*
import com.azure.feathr.pipeline.lookup.LookupSource
import com.azure.feathr.pipeline.operators.Divide
import com.azure.feathr.pipeline.operators.FunctionCall
import com.azure.feathr.pipeline.operators.Minus
import com.azure.feathr.pipeline.operators.Plus
import java.util.concurrent.CompletableFuture

class LookupTest {
    class TestLookupSource : LookupSource {
        private val columns: Map<String, Int> = mapOf("int_col1" to 0, "str_col2" to 1)
        private val store: Map<String, List<Value>> = mapOf(
            "k1" to listOf(Value(ColumnType.INT, 100), Value(ColumnType.STRING, "str_value1")),
            "k2" to listOf(Value(ColumnType.INT, 200), Value(ColumnType.STRING, "str_value2")),
            "k3" to listOf(Value(ColumnType.INT, 300), Value(ColumnType.STRING, "str_value3")),
            "k4" to listOf(Value(ColumnType.INT, 400), Value(ColumnType.STRING, "str_value4")),
        )
        override val sourceName: String
            get() = "test_lookup_source"

        override fun get(key: Value, fields: List<String>): CompletableFuture<List<Value?>> {
            val v = store[key.getString() ?: ""]
            val ret = fields.map {
                columns[it]?.let { idx ->
                    v?.get(idx)
                }
            }
            return CompletableFuture.completedFuture(ret)
        }

        override fun dump(): String {
            return "test_lookup_source"
        }
    }

    @Test
    fun testLookup() {
        val ls = TestLookupSource()

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

        // Lookup key: `"k" + to_string((intField2 - intField1)/90)`
        val exp = OperatorExpression(
            Plus(), listOf(
                ConstantExpression("k", ColumnType.STRING),
                OperatorExpression(
                    FunctionCall("to_string"), listOf(
                        OperatorExpression(
                            Divide(), listOf(
                                OperatorExpression(
                                    Minus(), listOf(
                                        GetColumnByIndex(1),
                                        GetColumn("intField1")
                                    )
                                ),
                                ConstantExpression(90, ColumnType.INT)
                            )
                        )
                    )
                )
            )
        )
        val tds = Lookup(listOf("int_col1", "str_col2"), exp, ls).apply { initialize(ds.getColumns()) }.transform(ds)

        // Now tds contains 4 columns: intField1, intField2, int_col1, str_col2, and 5 rows
        val r = runBlocking { tds.fetchAll().await() }
        assertEquals(5, r.size)
        assertEquals(100, r[0].getColumn(2).getInt())
        assertEquals(200, r[1].getColumn(2).getInt())
        assertEquals(300, r[2].getColumn(2).getInt())
        assertEquals(400, r[3].getColumn(2).getInt())
        assertEquals(null, r[4].getColumn(2).getInt())  // It's null because the key is not found

        assertEquals("str_value1", r[0].getColumn(3).getString())
        assertEquals("str_value2", r[1].getColumn(3).getString())
        assertEquals("str_value3", r[2].getColumn(3).getString())
        assertEquals("str_value4", r[3].getColumn(3).getString())
        assertEquals(null, r[4].getColumn(3).getString())
    }

    @Test
    fun timingTest() {
        val start =System.currentTimeMillis()
        repeat(100_000) {
            testLookup()
        }
        val stop = System.currentTimeMillis()
        println(stop - start)
    }
}