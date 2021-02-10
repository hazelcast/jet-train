package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.aggregate.AggregateOperations
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks

internal val stops = pipeline("stops", ToStop)
internal val agencies = pipeline("agency", ToAgency)
internal val routes = pipeline("routes", ToRoute)
internal val trips = pipeline("trips", ToTrip)
internal val stopTimes = Pipeline.create().apply {
    readFrom(file("stop_times"))
        .groupingKey { it.split(",")[0] }
        .aggregate(AggregateOperations.toList())
        .map(ToStopTime)
        .peek()
        .map(ToEntryForStopTime)
        .writeTo(Sinks.map("stop_times"))
}

private fun pipeline(name: String, toJson: FunctionEx<List<String>, String?>) =
    Pipeline.create().apply {
        readFrom(file(name))
            .apply(SplitByComma)
            .map(toJson)
            .peek()
            .map(ToEntry)
            .writeTo(Sinks.map(name))
    }