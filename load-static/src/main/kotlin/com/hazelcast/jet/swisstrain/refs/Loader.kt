package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.Jet
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.swisstrain.common.CloseableJet
import com.hazelcast.jet.swisstrain.common.withCloseable
import org.json.JSONObject

fun main() {

    Jet.newJetClient().withCloseable().use {
        with(config()) {
        submit(it, this, "agency", ToAgency())
        submit(it, this, "trips", ToTrip())
        submit(it, this, "routes", ToRoute())
        submit(it, this, "stops", ToStop())}
    }
}

private fun submit(
    jet: CloseableJet,
    config: JobConfig,
    name: String,
    withToJson: FunctionEx<List<String>, JSONObject?>
) =
    Pipeline.create().apply {
        drawFrom(file(name))
            .map(
                RemoveFirstAndLastChars()
                    .andThen(SplitByDoubleQuotes())
                    .andThen(RemoveDoubleQuotes())
                    .andThen(ToEntry(withToJson))
            )
            .drainTo(Sinks.map<String, String>(name))
        jet.newJob(this, config)
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
    ToTrip::class.java
)