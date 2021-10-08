package com.hazelcast.jettrain.refs

import com.hazelcast.client.HazelcastClient
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.config.JobConfig
import com.hazelcast.jet.pipeline.BatchStage
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jettrain.common.toStringFn
import com.hazelcast.jettrain.common.withCloseable

fun main() {
    execute(HazelcastClient.newHazelcastClient(), agencies, stops, routes, trips, stopTimes)
}

object SplitByComma : FunctionEx<BatchStage<String>, BatchStage<List<String>>> {
    override fun applyEx(stage: BatchStage<String>) =
        stage.map { it.split(",") }
}

internal fun execute(hazelcastInstance: HazelcastInstance, vararg pipeline: Pipeline) {
    hazelcastInstance.withCloseable().use { hz ->
        pipeline.forEach { pipeline ->
            hz.jet.newJob(pipeline, jobConfig).join()
        }
    }
}

internal val jobConfig = JobConfig()
    .addPackage(SplitByComma::class.java.`package`.name)
    .addPackage(toStringFn.javaClass.`package`.name)