package com.hazelcast.jettrain.data

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx

object TripIdExtractor : FunctionEx<JsonObject, String> {
    override fun applyEx(json: JsonObject): String {
        val tripId: String? = json
            .getAsJsonObject("vehicle")
            ?.getAsJsonObject("trip")
            ?.getAsJsonPrimitive("tripId")
            ?.asString
        return "${json.getAsJsonPrimitive("agencyId").asString}:$tripId"
    }
}

object MergeWithTrip : BiFunctionEx<JsonObject, String?, JsonObject> {
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, tripString: String?): JsonObject =
        if (tripString == null) json
        else json.apply {
            val trip = parser.parse(tripString).asJsonObject
            val headsign = trip.getAsJsonPrimitive("trip_headsign").asString
            getAsJsonObject("vehicle")
                .getAsJsonObject("trip")
                .addProperty("trip_headsign", headsign)
        }
}

object RouteIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject): String? {
        val routeId: String? = json
            .getAsJsonObject("vehicle")
            ?.getAsJsonObject("trip")
            ?.getAsJsonPrimitive("routeId")
            ?.asString
        return if (routeId == null) null
        else "${json.getAsJsonPrimitive("agencyId").asString}:$routeId"
    }
}

object MergeWithRoute : BiFunctionEx<JsonObject, String?, JsonObject> {
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, routeString: String?): JsonObject =
        if (routeString == null) json
        else json.apply {
            val route = parser.parse(routeString).asJsonObject
            getAsJsonObject("vehicle")
                .getAsJsonObject("trip").apply {
                    add("route", route)
                    remove("routeId")
                }
        }
}

object StopIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject): String? = json
        .getAsJsonObject("vehicle")
        ?.getAsJsonPrimitive("stopId")
        ?.asString
}

object MergeWithStop : BiFunctionEx<JsonObject, String?, JsonObject> {
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, stopString: String?): JsonObject =
        if (stopString == null) json
        else json.apply {
            val stop = parser.parse(stopString).asJsonObject
            getAsJsonObject("vehicle").apply {
                add("stop", stop)
                remove("stopId")
            }
        }
}