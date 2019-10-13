package com.hazelcast.jet.swisstrain.common

import com.hazelcast.jet.JetInstance
import java.io.Closeable

class CloseableJet(private val instance: JetInstance) : Closeable, JetInstance by instance {
    override fun close() {
        shutdown()
    }
}

fun JetInstance.withCloseable() = CloseableJet(this)

