package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks

internal val stops = pipeline("stops", ToStop)
internal val agencies = pipeline("agency", ToAgency)
internal val routes = pipeline("routes", ToRoute)
internal val trips = pipeline("trips", ToTrip)

private fun pipeline(name: String, jsonify: FunctionEx<List<String>, String?>) =
    Pipeline.create().apply {
        readFrom(file(name))
            .apply(CleanUp)
            .map(jsonify)
            .peek()
            .map(ToEntry)
            .writeTo(Sinks.map(name))
    }