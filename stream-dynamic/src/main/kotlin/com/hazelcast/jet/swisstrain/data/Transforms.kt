package com.hazelcast.jet.swisstrain.data

import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.function.ToLongFunctionEx
import java.io.Serializable
import java.util.*

class SplitPayload : FunctionEx<String, Traverser<JsonObject>> {
    override fun applyEx(payload: String): Traverser<JsonObject> {
        val json = Json.parse(payload).asObject()
        val entities = json.get("entity").asArray()
        return TripTraverser(entities)
    }
}

class TripTraverser(entities: JsonArray) : Traverser<JsonObject>, Serializable {
    private val iterator = entities.iterator()
    override fun next() =
        if (iterator.hasNext()) iterator.next().asObject()
        else null
}

class ToEntry : FunctionEx<JsonObject, Map.Entry<String, JsonObject>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, JsonObject> =
        AbstractMap.SimpleEntry(json.getString("id", null), json)
}

class TimestampExtractor : ToLongFunctionEx<String> {
    override fun applyAsLongEx(value: String): Long {
        val json = Json.parse(value).asObject()
        return json.get("header").asObject().getLong("timestamp", -1)
    }
}