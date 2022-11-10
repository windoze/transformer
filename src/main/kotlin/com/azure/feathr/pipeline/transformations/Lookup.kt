package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.*
import com.azure.feathr.pipeline.lookup.LookupSource
import com.azure.feathr.pipeline.lookup.LookupSourceRepo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import kotlin.math.min

class Lookup(
    private val valueColumns: List<String>,
    private val keyExpression: Expression,
    private val lookupDataSet: LookupSource,
    private val batchSize: Int = 100,
) : Transformation {
    constructor(
        valueColumns: List<String>,
        keyExpression: Expression,
        lookupDataSetName: String,
        batchSize: Int = 100
    ) : this(
        valueColumns,
        keyExpression,
        if (LookupSourceRepo.contains(lookupDataSetName)) LookupSourceRepo[lookupDataSetName]
        else throw InvalidReference(lookupDataSetName),
        batchSize
    )

    override fun transform(input: DataSet): DataSet {
        return LookupDataSet(
            input,
            getOutputSchema(input.getColumns()),
            valueColumns,
            keyExpression,
            lookupDataSet,
            batchSize
        )
    }

    override fun getOutputSchema(inputColumns: List<Column>): List<Column> {
        return inputColumns + valueColumns.map {
            Column(
                it,
                ColumnType.DYNAMIC
            )
        }
    }

    override fun initialize(columns: List<Column>) {
        keyExpression.initialize(columns)
    }

    override fun dump(): String {
        return "lookup ${valueColumns.joinToString(", ")} from ${lookupDataSet.dump()} on ${keyExpression.dump()}"
    }

    class LookupDataSet(
        private val input: DataSet,
        private val transformedColumns: List<Column>,
        private val valueColumns: List<String>,
        private val keyExpression: Expression,
        private val lookupDataSet: LookupSource,
        private val batchSize: Int,
    ) : DataSet {
        private val keyNotFound = List(valueColumns.size) {
            null
        }

        override fun getColumns(): List<Column> {
            return transformedColumns
        }

        override fun fetch(length: Int): CompletableFuture<List<Row>> {
            return CoroutineScope(SupervisorJob()).future { fetchAsync(length) }
        }

        override fun fetchAll(): CompletableFuture<List<Row>> {
            return CoroutineScope(SupervisorJob()).future { fetchAsync(Int.MAX_VALUE) }
        }

        private suspend fun fetchAsync(length: Int): List<Row> {
            val ret: MutableList<Row> = mutableListOf()
            while (ret.size < length) {
                val batchSize = min(length - ret.size, batchSize)
                val batch = input.fetch(batchSize).await()
                if (batch.isEmpty()) break
                val keys = batch.map {
                    keyExpression.evaluate(it)
                }
                val values = lookupDataSet.batchGet(keys.toSet(), valueColumns).await()
                ret.addAll(batch.zip(keys) { row, key ->
                    EagerRow(
                        transformedColumns,
                        row.evaluate().map { it?.getDynamic() } + values.getOrDefault(key, keyNotFound).map {
                            it?.getDynamic()
                        }
                    )
                })
            }
            return ret
        }
    }
}