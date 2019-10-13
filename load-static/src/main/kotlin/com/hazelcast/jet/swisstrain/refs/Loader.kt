package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.swisstrain.common.withCloseable
import org.json.JSONObject

fun main() {

    Jet.newJetClient().withCloseable().use {
        with(config()) {
            it.newJob(pipeline("agency", ToAgency()), this).join()
            it.newJob(pipeline("routes", ToRoute()), this).join()
            it.newJob(pipeline("stops", ToStop()), this).join()
            it.newJob(pipeline("trips", ToTrip()), this).join()
            it.newJob(pipeline("stop_times", ToStopTime()), this)
        }
    }
}

private fun pipeline(
    name: String,
    jsonify: FunctionEx<List<String>, JSONObject?>,
    merge: FunctionEx<JSONObject?, JSONObject?> = FunctionEx.identity())=
    Pipeline.create().apply {
        drawFrom(file(name))
            .map(
                RemoveFirstAndLastChars()
                    .andThen(SplitByDoubleQuotes())
                    .andThen(RemoveDoubleQuotes())
                    .andThen(jsonify)
                    .andThen(merge)
                    .andThen(ToEntry())
            )
            .drainTo(Sinks.map<String, String>(name))
    }

private fun config() = JobConfig().addClass(
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
    ToStopTime::class.java
)