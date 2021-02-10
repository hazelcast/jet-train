package com.hazelcast.jettrain.data

import com.hazelcast.jet.impl.JetBootstrap
import com.hazelcast.jettrain.common.withCloseable

fun main(vararg args: String) {
    JetBootstrap.getInstance().withCloseable().use {
        it.newJob(pipeline(args[0]), jobConfig)
    }
}