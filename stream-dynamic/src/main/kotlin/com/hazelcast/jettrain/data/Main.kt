package com.hazelcast.jettrain.data

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.ServiceFactories
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jettrain.common.sampleEvery
import com.hazelcast.jettrain.common.toStringFn
import com.hazelcast.jettrain.common.withCloseable

fun main(vararg args: String) {
    HazelcastClient.newHazelcastClient().withCloseable().use {
        it.jet.newJob(pipeline(args[0]), jobConfig)
    }
}

internal fun pipeline(token: String) = Pipeline.create().apply {
    val service = remoteService(token)
    readFrom(service)
        .withIngestionTimestamps()
        .flatMap(ToEntities)
        .map(ToJson)
        .filter(OnlyEntityWithTrip.and(OnlyEntityWithStop))
        .mapUsingIMap("stop_times", TripIdExtractor, EnrichWithStopTimes)
        .mapUsingIMap("trips", TripIdExtractor, EnrichWithTrip)
        .mapUsingIMap("routes", RouteIdExtractor, EnrichWithRoute)
        .mapUsingService(
            ServiceFactories.iMapService("stops"),
            EnrichWithStop
        ).map(TimeToTimestamps)
        .map(ToFlattenedStructure)
        .peek(sampleEvery(50), toStringFn)
        .map(ToEntry)
        .writeTo(Sinks.remoteMap("update", ClientConfig()))
}

internal val jobConfig = JobConfig()
    .addPackage(ToEntities::class.java.`package`.name)
    .addPackage(toStringFn.javaClass.`package`.name)