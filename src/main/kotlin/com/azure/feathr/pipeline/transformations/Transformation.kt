package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.Column
import com.azure.feathr.pipeline.DataSet
import com.azure.feathr.pipeline.Initializable

interface Transformation: Initializable {
    fun getOutputSchema(inputColumns: List<Column>): List<Column>

    fun transform(input: DataSet): DataSet

    fun dump(): String
}