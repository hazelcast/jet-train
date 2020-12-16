package com.hazelcast.jet.swisstrain.data

import com.google.gson.JsonParser
import com.google.protobuf.util.JsonFormat
import com.google.transit.realtime.GtfsRealtime
import com.google.transit.realtime.GtfsRealtime.FeedEntity
import com.hazelcast.function.FunctionEx
import com.hazelcast.function.ToLongFunctionEx
import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Traverser
import java.io.Serializable
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle
import java.util.*

object SplitPayload : FunctionEx<Pair<String, ByteArray>, Traverser<Pair<String, FeedEntity>>> {
    override fun applyEx(payload: Pair<String, ByteArray>): Traverser<Pair<String, FeedEntity>> {
        val message = GtfsRealtime.FeedMessage.parseFrom(payload.second)
        return TripTraverser(payload.first, message.entityList)
    }
}

class TripTraverser(private val agency: String, entities: MutableList<FeedEntity>) : Traverser<Pair<String, FeedEntity>>, Serializable {
    private val iterator = entities.iterator()
    override fun next() =
        if (iterator.hasNext()) agency to iterator.next()
        else null
}

object ProtobufToJsonWithAgency : FunctionEx<Pair<String, FeedEntity>, com.google.gson.JsonObject> {
    override fun applyEx(t: Pair<String, FeedEntity>): com.google.gson.JsonObject {
        val string = JsonFormat.printer().print(t.second)
        return JsonParser()
            .parse(string)
            .asJsonObject
            .apply {
                addProperty("agencyId", t.first)
            }
    }
}

object HourToTimestamp : FunctionEx<JsonObject, JsonObject> {

    private const val pattern = "HH:mm:ss"

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
            .atZone(ZoneId.of("Europe/Paris"))
            .toInstant()
            .epochSecond
    }
}

object ToEntry : FunctionEx<JsonObject, Map.Entry<String, JsonObject>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, JsonObject> =
        AbstractMap.SimpleEntry(json.getString("id", null), json)
}

object TimestampExtractor : ToLongFunctionEx<String> {
    override fun applyAsLongEx(value: String): Long {
        val json = Json.parse(value).asObject()
        return json.get("header").asObject().getLong("timestamp", -1)
    }
}