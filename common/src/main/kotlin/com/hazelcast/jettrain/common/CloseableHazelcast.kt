package com.hazelcast.jettrain.common

import com.hazelcast.core.HazelcastInstance
import java.io.Closeable

class CloseableHazelcast(private val instance: HazelcastInstance) : Closeable, HazelcastInstance by instance {
    override fun close() {
        instance.shutdown()
    }
}

fun HazelcastInstance.withCloseable() = CloseableHazelcast(this)