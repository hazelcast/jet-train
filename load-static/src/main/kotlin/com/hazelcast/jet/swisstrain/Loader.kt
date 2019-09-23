package com.hazelcast.jet.swisstrain

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.jet.Jet
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.function.PredicateEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import org.json.JSONObject
import java.util.*


fun main() {
    Jet.newJetInstance().withCloseable().use {
        it.newJob(pipeline("stops", ::toStop)).join()
        it.newJob(pipeline("agency", ::toAgency)).join()
        it.newJob(pipeline("routes", ::toRoute)).join()
        it.newJob(pipeline("trips", ::toTrip)).join()
    }
}

private fun pipeline(name: String, toJson: (List<String>) -> JSONObject?) = Pipeline.create()
    .apply {
        drawFrom(file(name))
            .map(
                removeFirstAndLastChars()
                    .andThen(splitByDoubleQuotes())
                    .andThen(removeDoubleQuotes())
                    .andThen(jsonify(toJson))
            )
            .filter(isNotNull())
            .map(toEntry())
            .drainTo(Sinks.remoteMap<String, String>(name, ClientConfig()))
    }

private fun removeFirstAndLastChars() = FunctionEx<String, String> { it.substring(1, it.length - 1) }
private fun splitByDoubleQuotes() = FunctionEx<String, List<String>> { it.split("\",\"") }
private fun removeDoubleQuotes() = FunctionEx<List<String>, List<String>> { it.map { it.replace("\"", "") } }
private fun jsonify(toJson: (List<String>) -> JSONObject?) = FunctionEx<List<String>, JSONObject> { toJson.invoke(it) }

private fun isNotNull() = PredicateEx<JSONObject> { it != null }

private fun JSONObject.toEntry(): Map.Entry<String, String> = AbstractMap.SimpleEntry(getString("id"), toString())
private fun toEntry() = FunctionEx<JSONObject, Map.Entry<String, String>> { it.toEntry() }