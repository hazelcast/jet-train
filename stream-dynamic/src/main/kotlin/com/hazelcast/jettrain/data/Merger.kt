package com.hazelcast.jettrain.data

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.map.IMap

object TripIdExtractor : FunctionEx<JsonObject, String> {
    override fun applyEx(json: JsonObject): String {
        val tripId = json
            .getAsJsonObject("vehicle")
            ?.getAsJsonObject("trip")
            ?.getAsJsonPrimitive("tripId")
            ?.asString
        return "${json.getAsJsonPrimitive("agencyId").asString}:$tripId"
    }
}

object EnrichWithTrip : BiFunctionEx<JsonObject, String?, JsonObject> {
    override fun applyEx(json: JsonObject, tripString: String?) =
        if (tripString == null) json
        else json.apply {
            val trip = JsonParser.parseString(tripString).asJsonObject
            val headsign = trip.getAsJsonPrimitive("trip_headsign").asString
            getAsJsonObject("vehicle")
                .getAsJsonObject("trip")
                .addProperty("trip_headsign", headsign)
        }
}

object RouteIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject): String {
        val routeId = json
            .getAsJsonObject("vehicle")
            ?.getAsJsonObject("trip")
            ?.getAsJsonPrimitive("routeId")
            ?.asString
        return "${json.getAsJsonPrimitive("agencyId").asString}:$routeId"
    }
}

object EnrichWithRoute : BiFunctionEx<JsonObject, String?, JsonObject> {
    override fun applyEx(json: JsonObject, routeString: String?) =
        if (routeString == null) json
        else json.apply {
            val route = JsonParser.parseString(routeString).asJsonObject
            getAsJsonObject("vehicle")
                .getAsJsonObject("trip").apply {
                    add("route", route)
                    remove("routeId")
                }
        }
}

object EnrichWithStopTimes : BiFunctionEx<JsonObject, String?, JsonObject> {
    override fun applyEx(json: JsonObject, scheduleString: String?) =
        if (scheduleString == null) json
        else json.apply {
            val schedule = JsonParser.parseString(scheduleString).asJsonArray
            add("schedule", schedule)
        }
}

object EnrichWithStop : BiFunctionEx<IMap<String, String>, JsonObject, JsonObject> {
    override fun applyEx(stops: IMap<String, String>, json: JsonObject): JsonObject {
        return json.apply {
            val vehicle = json.getAsJsonObject("vehicle")
            val mainStopId = vehicle
                ?.getAsJsonPrimitive("stopId")
                ?.asString
            stops[mainStopId]?.let {
                add("stop", JsonParser.parseString(it))
                remove("stopId")
            }
            val schedule = getAsJsonArray("schedule")
            val newSchedule = schedule?.map {
                val element = it as JsonObject
                val stopId = element["stopId"].asString
                val stopString = stops[stopId]
                if (stopString == null) it
                else it.apply {
                    add("stop", JsonParser.parseString(stopString))
                    remove("stopId")
                }
            }?.fold(JsonArray()) { acc, element ->
                acc.add(element)
                acc
            }
            add("schedule", newSchedule)
        }
    }
}