package com.azure.feathr.pipeline.transformations

import com.azure.feathr.pipeline.Initializable

interface Transformation: Initializable {
    fun getOutputSchema(inputColumns: List<com.azure.feathr.pipeline.Column>): List<com.azure.feathr.pipeline.Column>

    fun transform(input: com.azure.feathr.pipeline.DataSet): com.azure.feathr.pipeline.DataSet

    fun dump(): String
}