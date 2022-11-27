package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.Value
import java.util.Random

object RandFunctions {
    private val generator = Random()

    fun rand(): Double {
        return generator.nextDouble()
    }

    fun shuffle(l: List<Value>): List<Value> {
        return l.shuffled()
    }
}