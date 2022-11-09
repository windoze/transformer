package com.azure.feathr.pipeline

interface Initializable {
    /**
     * All preparations should be done in `instantiate()`
     * The whole program assumes that no internal state will be ever changed again after this function call
     */
    fun initialize(columns: List<Column>) {
        // Do nothing by default
    }
}