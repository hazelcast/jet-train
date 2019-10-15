package com.hazelcast.jet.swisstrain.data

import com.hazelcast.internal.json.Json
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.function.FunctionEx
import java.io.Serializable
import java.util.*

class SplitPayload : FunctionEx<String, Traverser<String>> {
    override fun applyEx(payload: String): Traverser<String> {
        try {
            val json = Json.parse(payload).asObject()
            val entities = json.get("entity").toString()
            return TripTraverser(entities)
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }
}

class TripTraverser(entities: String) : Traverser<String>, Serializable {
    private val iterator = Json.parse(entities).asArray().iterator()
    override fun next() =
        if (iterator.hasNext()) iterator.next().toString()
        else null
}

class ToEntry : FunctionEx<String, Map.Entry<String, String>> {
    override fun applyEx(value: String): Map.Entry<String, String> {
        with(Json.parse(value).asObject()) {
            return AbstractMap.SimpleEntry(get("id").asString(), toString())
        }
    }
}