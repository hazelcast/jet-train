package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.swisstrain.common.withCloseable

fun main() {

    Jet.newJetClient().withCloseable().use {
        with(config) {
            it.newJob(pipeline("stops", ToStop), this).join()
            it.newJob(pipeline("agency", ToAgency), this).join()
            it.newJob(
                pipeline(
                    "routes",
                    ToRoute,
                    Triple("agency", GetAgencyId, MergeWithAgency)
                ), this
            ).join()
            it.newJob(
                pipeline(
                    "trips",
                    ToTrip,
                    Triple("routes", GetRouteId, MergeWithRoute)
                ), this
            ).join()
            it.newJob(pipeline("stop_times", ToStopTime), this).join()
        }
    }
}

private fun pipeline(
    name: String,
    jsonify: FunctionEx<List<String>, JsonObject?>,
    mergeWith: Triple<String, IdExtractorFn, BiFunctionEx<JsonObject?, JsonObject?, JsonObject?>>? = null
) =
    Pipeline.create().apply {
        val commonMap = drawFrom(file(name))
            .map(
                RemoveFirstAndLastChars
                    .andThen(SplitByDoubleQuotes)
                    .andThen(RemoveDoubleQuotes)
                    .andThen(jsonify)
            )
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
            .drainTo(Sinks.map<Any, JsonObject>(name))
    }

private val config = JobConfig()
    .addClass(
        CreateReader::class.java,
        FillBuffer::class.java,
        CloseReader::class.java,
        RemoveFirstAndLastChars::class.java,
        SplitByDoubleQuotes::class.java,
        RemoveDoubleQuotes::class.java,
        ToJson::class.java,
        ToEntry::class.java,
        ToStop::class.java,
        ToAgency::class.java,
        ToRoute::class.java,
        ToTrip::class.java,
        ToStopTime::class.java,
        IdExtractorFn::class.java,
        GetAgencyId::class.java,
        GetRouteId::class.java,
        MergeWithAgency::class.java,
        MergeWithRoute::class.java
    )