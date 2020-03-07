package com.hazelcast.jet.swisstrain.data

import com.hazelcast.jet.impl.JetBootstrap
import com.hazelcast.jet.swisstrain.common.withCloseable

fun main() {
    JetBootstrap.getInstance().withCloseable().use {
        it.newJob(pipeline(), jobConfig)
    }
}