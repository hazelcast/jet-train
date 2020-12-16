package com.hazelcast.jettrain.data

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jettrain.common.withCloseable

fun main() {
    Jet.newJetClient().withCloseable().use {
        it.newJob(pipeline(), jobConfig)
    }
}

internal fun pipeline() = Pipeline.create().apply {
    val service = remoteService(token = System.getProperty("token"))
    readFrom(service)
//        .withTimestamps(TimestampExtractor, 200)
        .flatMap(SplitPayload)
        .map(ProtobufToJsonWithAgency)
        .filter(OnlyEntityWithTrip.and(OnlyEntityWithStop))
        .mapUsingIMap("trips", TripIdExtractor, MergeWithTrip)
        .mapUsingIMap("routes", RouteIdExtractor, MergeWithRoute)
        .mapUsingIMap("stops", StopIdExtractor, MergeWithStop)
        .peek()
        .map(ToEntry)
        .writeTo(Sinks.remoteMap("update", clientConfig))
}

internal val clientConfig = ClientConfig().apply {
    clusterName = "jet"
}

internal val jobConfig = JobConfig().addPackage(SplitPayload::class.java.`package`.name)