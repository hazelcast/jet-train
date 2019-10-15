package com.hazelcast.jet.swisstrain.data

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.swisstrain.common.withCloseable

internal const val URL = "https://api.opentransportdata.swiss/gtfs-rt?format=JSON"

fun main() {
    Jet.newJetClient().withCloseable().use {
        it.newJob(pipeline(), jobConfig)
    }
}

private fun pipeline() = Pipeline.create().apply {
    val token = System.getProperty("token")
    drawFrom(remoteService(URL, token))
        .withIngestionTimestamps()
        .flatMap(SplitPayload())
        .peek()
        .map(ToEntry())
        .drainTo(Sinks.remoteMap<String, String>("update", clientConfig))
}

private val clientConfig = ClientConfig().apply {
    groupConfig.name = "jet"
}

private val jobConfig = JobConfig()
    .addClass(
        SplitPayload::class.java,
        TripTraverser::class.java,
        FillBuffer::class.java,
        CreateContext::class.java,
        TimeHolder::class.java,
        ToEntry::class.java
    )