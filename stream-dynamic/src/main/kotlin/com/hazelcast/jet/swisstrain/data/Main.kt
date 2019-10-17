package com.hazelcast.jet.swisstrain.data

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.function.PredicateEx
import com.hazelcast.jet.pipeline.ContextFactory
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
        .filter(Taker(10))
        .mapUsingContext(
            ContextFactory.withCreateFn(ContextCreator()),
            MergeWithTrip()
        )
        .mapUsingContext(
            ContextFactory.withCreateFn(ContextCreator()),
            MergeWithStops()
        )
        .map(AdjustTimeWithDelays())
        .map(ComputeLocation())
        .peek()
        .map(ToEntry())
        .drainTo(Sinks.remoteMap<String, JsonObject>("update", clientConfig))
}

class Taker(private val limit: Int): PredicateEx<JsonObject> {
    private var i = 0
    override fun testEx(json: JsonObject): Boolean {
        i++
        return i < limit
    }
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
        ToEntry::class.java,
        ContextCreator::class.java,
        MergeWithTrip::class.java,
        MergeWithStops::class.java,
        AdjustTimeWithDelays::class.java,
        ComputeLocation::class.java,
        Taker::class.java
    )