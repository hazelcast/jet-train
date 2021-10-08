package com.hazelcast.jettrain.data

import com.hazelcast.core.Hazelcast
import com.hazelcast.jettrain.common.withCloseable

fun main(vararg args: String) {
    Hazelcast.bootstrappedInstance().withCloseable().use {
        it.jet.newJob(pipeline(args[0]), jobConfig)
    }
}