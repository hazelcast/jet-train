package com.hazelcast.jet.swisstrain.data

import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.function.ToLongFunctionEx
import java.io.Serializable
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle
import java.util.*

class SplitPayload : FunctionEx<String, Traverser<JsonObject>> {
    override fun applyEx(payload: String): Traverser<JsonObject> {
        val json = Json.parse(payload).asObject()
        val timestamp = json.get("header").asObject().getLong("timestamp", 0)
        val entities = json.get("entity").asArray()
        return TripTraverser(entities, timestamp)
    }
}

class TripTraverser(entities: JsonArray, private val timestamp: Long) : Traverser<JsonObject>, Serializable {
    private val iterator = entities.iterator()
    override fun next() =
        if (iterator.hasNext()) iterator.next().asObject().add("timestamp", timestamp)
        else null
}

class HourToTimestamp : FunctionEx<JsonObject, JsonObject> {

    private val pattern = "HH:mm:ss"

    override fun applyEx(json: JsonObject): JsonObject {
        val schedule = json
            .get("schedule")
            .asArray()
            .map { it as JsonObject }
            .map {
                JsonObject(it)
                    .set("arrival", it.getString("arrival", null).toTimestamp())
                    .set("departure", it.getString("departure", null).toTimestamp())
            }.fold(JsonArray()) { accumulator, current ->
                accumulator.add(current)
            }
        return JsonObject(json).set("schedule", schedule)
    }

    private fun String.toTimestamp(): Long {
        val formatter = DateTimeFormatter
            .ofPattern(pattern)
            .withResolverStyle(ResolverStyle.LENIENT)
        return LocalTime
            .from(formatter.parse(this))
            .atDate(LocalDate.now())
            .atZone(ZoneId.of("GMT"))
            .toInstant()
            .epochSecond
    }
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