package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks

internal val stops = pipeline("stops", ToStop)
internal val agencies = pipeline("agency", ToAgency)
internal val routes = pipeline(
    "routes",
    ToRoute,
    Triple("agency", GetAgencyId, MergeWithAgency)
)
internal val trips = pipeline(
    "trips",
    ToTrip,
    Triple("routes", GetRouteId, MergeWithRoute)
)
internal val stopTimes = pipeline("stop_times", ToStopTime)

private fun pipeline(
    name: String,
    jsonify: FunctionEx<List<String>, String?>,
    mergeWith: Triple<String, IdExtractorFn, BiFunctionEx<String?, String?, String?>>? = null
) =
    Pipeline.create().apply {
        val commonMap = readFrom(file(name))
            .apply(CleanUp)
            .map(jsonify)
        val richMap =
            if (mergeWith != null) {
                commonMap.mapUsingIMap(
                    mergeWith.first,
                    mergeWith.second,
                    mergeWith.third
                )
            } else commonMap
        richMap
            .peek()
            .map(ToEntry)
            .writeTo(Sinks.map(name))
    }