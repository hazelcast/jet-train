package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.Jet
import com.hazelcast.jet.JetInstance
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.BatchStage
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.swisstrain.common.withCloseable

fun main() {
    execute(Jet.newJetClient(), stops, agencies, routes, trips)
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
        stage.map { it.split(",") }
}

internal val jobConfig = JobConfig().addPackage(CleanUp::class.java.`package`.name)