package com.hazelcast.jettrain.data

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
        return JsonParser()
            .parse(string)
            .asJsonObject
            .apply {
                addProperty("agencyId", payload.first)
            }
    }
}

object OnlyEntityWithTrip : PredicateEx<JsonObject> {
    override fun testEx(json: JsonObject) = json
        .get("vehicle")
        .asJsonObject
        .has("trip")
}

object OnlyEntityWithStop : PredicateEx<JsonObject> {
    override fun testEx(json: JsonObject) = json
        .get("vehicle")
        .asJsonObject
        .has("stopId")
}

object OnlyEntityWithRouteId : PredicateEx<JsonObject> {
    override fun testEx(json: JsonObject) = json
        .getAsJsonObject("vehicle")
        ?.getAsJsonObject("trip")
        ?.getAsJsonPrimitive("routeId")
        ?.asString
        ?.isNotEmpty()
        ?: false
}

object ToEntry : FunctionEx<JsonObject, Map.Entry<String, String>> {
    override fun applyEx(json: JsonObject): MutableMap.MutableEntry<String, String> =
        Util.entry(
            json.getAsJsonPrimitive("id").asString,
            json.toString()
        )
}

object TimestampExtractor : ToLongFunctionEx<String> {
    private val parser = JsonParser()
    override fun applyAsLongEx(value: String) = parser
        .parse(value)
        .asJsonObject
        .getAsJsonObject("header")
        .getAsJsonPrimitive("timestamp")
        .asLong
}