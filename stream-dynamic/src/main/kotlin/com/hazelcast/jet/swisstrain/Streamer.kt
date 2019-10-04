package com.hazelcast.jet.swisstrain

import com.hazelcast.jet.Jet
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.function.PredicateEx
import com.hazelcast.jet.pipeline.Pipeline

private const val URL = "https://api.opentransportdata.swiss/gtfs-rt?format=JSON"

fun main() {
    Jet.newJetInstance().withCloseable().use {
        it.newJob(pipeline()).join()
    }
}

private fun pipeline() = Pipeline.create().apply {
    val token = System.getProperty("token")
    drawFrom(remoteService(URL, token))
        .withoutTimestamps()
        .filter(skipFirstLine())
        .map(removeTrailingComma())
        .drainTo(queue())
}

private fun skipFirstLine() = PredicateEx<String> { it.startsWith("{\"id\":\"") }
private fun removeTrailingComma() = FunctionEx<String, String> { it.removeSuffix(",") }