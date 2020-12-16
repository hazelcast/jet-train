package com.hazelcast.jettrain.data

import com.hazelcast.jet.impl.JetBootstrap
import com.hazelcast.jettrain.common.withCloseable

fun main() {
    JetBootstrap.getInstance().withCloseable().use {
        it.newJob(pipeline(), jobConfig)
    }
}