package com.hazelcast.jettrain.data

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.ServiceFactories
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jettrain.common.withCloseable

fun main(vararg args: String) {
    Jet.newJetClient().withCloseable().use {
        it.newJob(pipeline(args[0]), jobConfig)
    }
}

internal fun pipeline(token: String) = Pipeline.create().apply {
    val service = remoteService(token)
    readFrom(service)
//        .withTimestamps(TimestampExtractor, 200)
        .flatMap(ToEntities)
        .map(ToJson)
        .filter(OnlyEntityWithTrip.and(OnlyEntityWithStop))
        .mapUsingIMap("stop_times", TripIdExtractor, EnrichWithStopTimes)
        .mapUsingIMap("trips", TripIdExtractor, EnrichWithTrip)
        .mapUsingIMap("routes", RouteIdExtractor, EnrichWithRoute)
        .mapUsingService(
            ServiceFactories.iMapService("stops"),
            EnrichWithStop
        )
        .peek()
        .map(ToEntry)
        .writeTo(Sinks.remoteMap("update", clientConfig))
}

internal val clientConfig = ClientConfig().apply {
    clusterName = "jet"
}

internal val jobConfig = JobConfig().addPackage(ToEntities::class.java.`package`.name)