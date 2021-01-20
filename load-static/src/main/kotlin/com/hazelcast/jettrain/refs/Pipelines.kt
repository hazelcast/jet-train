package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.Util
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks

internal val stops = pipeline("stops", ToStop)
internal val agencies = pipeline("agency", ToAgency)
internal val routes = pipeline("routes", ToRoute)
internal val trips = pipeline("trips", ToTrip)
internal val stopTimes = Pipeline.create().apply {
    readFrom(file("stop_times"))
        .groupingKey { it.split(",")[0] }
        .mapStateful(
            { mutableMapOf() },
            { state: MutableMap<String, MutableList<String>>, key: String, row: String ->
                if (state.containsKey(key)) state[key]?.add(row)
                else state[key] = mutableListOf(row)
                state
            })
        .flatMap(MapTraverser)
        .map(ToStopTime)
        .distinct()
        .peek()
        .map { json -> Util.entry(json.get("id").asString(), json.get("schedule").asArray().toString()) }
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