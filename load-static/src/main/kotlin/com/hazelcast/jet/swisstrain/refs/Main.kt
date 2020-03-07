package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.Jet
import com.hazelcast.jet.JetInstance
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.BatchStage
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.swisstrain.common.withCloseable

fun main() {
    execute(Jet.newJetClient(), stops, agencies, routes, trips, stopTimes)
}

internal fun execute(jetInstance: JetInstance, vararg pipeline: Pipeline) {
    jetInstance.withCloseable().use { jet ->
        pipeline.forEach { pipeline ->
            jet.newJob(pipeline, jobConfig).join()
        }
    }
}

object CleanUp : FunctionEx<BatchStage<String>, BatchStage<List<String>>> {
    override fun applyEx(stage: BatchStage<String>) =
        stage.map(
            RemoveFirstAndLastChars
                .andThen(SplitByDoubleQuotes)
                .andThen(RemoveDoubleQuotes)
        )
}

private val jobConfig = JobConfig()
    .addClass(
        CleanUp::class.java,
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