package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.Jet
import com.hazelcast.jet.JetInstance
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.BatchStage
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jettrain.common.withCloseable

fun main() {
    execute(Jet.newJetClient(), agencies, stops, routes, trips, stopTimes)
}

object SplitByComma : FunctionEx<BatchStage<String>, BatchStage<List<String>>> {
    override fun applyEx(stage: BatchStage<String>) =
        stage.map { it.split(",") }
}

internal fun execute(jetInstance: JetInstance, vararg pipeline: Pipeline) {
    jetInstance.withCloseable().use { jet ->
        pipeline.forEach { pipeline ->
            jet.newJob(pipeline, jobConfig).join()
        }
    }
}

internal val jobConfig = JobConfig().addPackage(SplitByComma::class.java.`package`.name)