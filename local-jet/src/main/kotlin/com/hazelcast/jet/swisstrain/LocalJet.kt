package com.hazelcast.jet.swisstrain

import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JetConfig

fun main() {
    Jet.newJetInstance(JetConfig().apply {
        metricsConfig.isMetricsForDataStructuresEnabled = true
    })
}