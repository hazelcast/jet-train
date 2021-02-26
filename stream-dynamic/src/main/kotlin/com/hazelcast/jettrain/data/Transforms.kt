package com.hazelcast.jettrain.data

import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.util.JsonFormat
import com.google.transit.realtime.GtfsRealtime
import com.google.transit.realtime.GtfsRealtime.FeedEntity
import com.hazelcast.function.FunctionEx
import com.hazelcast.function.PredicateEx
import com.hazelcast.function.ToLongFunctionEx
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.Util
import java.io.Serializable
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

object ToEntities : FunctionEx<Pair<String, ByteArray>, Traverser<Pair<String, FeedEntity>>> {
    override fun applyEx(payload: Pair<String, ByteArray>): Traverser<Pair<String, FeedEntity>> {
        val message = GtfsRealtime.FeedMessage.parseFrom(payload.second)
        return TripTraverser(payload.first, message.entityList)
    }
}

class TripTraverser(private val agency: String, entities: MutableList<FeedEntity>) :
    Traverser<Pair<String, FeedEntity>>, Serializable {
    private val iterator = entities.iterator()
    override fun next() =
        if (iterator.hasNext()) agency to iterator.next()
        else null
}

object ToJson : FunctionEx<Pair<String, FeedEntity>, JsonObject> {
    override fun applyEx(payload: Pair<String, FeedEntity>): JsonObject {
        val string = JsonFormat.printer().print(payload.second)
        return JsonParser
            .parseString(string)
            .asJsonObject
            .apply {
                addProperty("agencyId", payload.first)
            }
    }
}

object OnlyEntityWithTrip : PredicateEx<JsonObject> {
    override fun testEx(json: JsonObject) = json
        .getAsJsonObject("vehicle")
        .has("trip")
}

object OnlyEntityWithStop : PredicateEx<JsonObject> {
    override fun testEx(json: JsonObject) = json
        .getAsJsonObject("vehicle")
        .has("stopId")
}

object ToEntry : FunctionEx<JsonObject, Map.Entry<String, String>> {
    override fun applyEx(json: JsonObject): MutableMap.MutableEntry<String, String> =
        Util.entry(
            json.getAsJsonPrimitive("id").asString,
            json.toString()
        )
}

object TimeToTimestamps : FunctionEx<JsonObject, JsonObject> {
    private const val pattern = "HH:mm:ss"
    override fun applyEx(json: JsonObject): JsonObject {
        val initialSchedule = json.get("schedule")
        val schedule = if (initialSchedule is JsonNull) JsonArray()
            else initialSchedule.asJsonArray
            .map { it as JsonObject }
            .map {
                JsonObject().apply {
                    val arrival = it.getAsJsonPrimitive("arrival")?.asString?.toTimestamp()
                    val departure = it.getAsJsonPrimitive("departure")?.asString?.toTimestamp()
                    addProperty("arrival", arrival)
                    addProperty("departure", departure)
                    add("sequence", it.get("sequence"))
                    add("stop", it.get("stop"))
                }
            }.fold(JsonArray()) { jsonArray, currentJson ->
                jsonArray.apply { add(currentJson) }
            }
        json.add("schedule", schedule)
        return json
    }

    private fun String.toTimestamp(): Long {
        val formatter = DateTimeFormatter
            .ofPattern(pattern)
            .withResolverStyle(ResolverStyle.LENIENT)
        return LocalTime
            .from(formatter.parse(this))
            .atDate(LocalDate.now())
            .atZone(ZoneId.of("America/Los_Angeles"))
            .toInstant()
            .epochSecond
    }
}