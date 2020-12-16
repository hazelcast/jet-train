package com.hazelcast.jet.swisstrain.data

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.swisstrain.common.withCloseable

fun main() {
    Jet.newJetClient().withCloseable().use {
        it.newJob(pipeline(), jobConfig)
    }
}

internal fun pipeline() = Pipeline.create().apply {
    val service = remoteService(token = System.getProperty("token"))
    readFrom(service)
        .withTimestamps(TimestampExtractor, 200)
        .flatMap(SplitPayload)
        .map(ProtobufToJsonWithAgency)
        .mapUsingIMap("trips", TripIdExtractor, MergeWithTrip)
        .writeTo(Sinks.logger { it.toString() })
            ServiceFactories.iMapService("stop_times"),
            MergeWithStopTimes
        )
        .map(HourToTimestamp)
        .mapUsingService(
            ServiceFactories.iMapService("stops"),
            MergeWithLocation
        ).peek()
        .map(ToEntry)
        .writeTo(Sinks.remoteMap("update", clientConfig))
}

internal val clientConfig = ClientConfig().apply {
    clusterName = "jet"
}

internal val jobConfig = JobConfig().addPackage(SplitPayload::class.java.`package`.name)