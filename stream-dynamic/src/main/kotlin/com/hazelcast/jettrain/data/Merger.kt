package com.hazelcast.jettrain.data

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx

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
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, tripString: String?) =
        json.apply {
            val trip = parser.parse(tripString).asJsonObject
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
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, routeString: String?) =
        json.apply {
            val route = parser.parse(routeString).asJsonObject
            getAsJsonObject("vehicle")
                .getAsJsonObject("trip").apply {
                    add("route", route)
                    remove("routeId")
                }
        }
}

object StopIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject) = json
        .getAsJsonObject("vehicle")
        ?.getAsJsonPrimitive("stopId")
        ?.asString
}

object EnrichWithStop : BiFunctionEx<JsonObject, String?, JsonObject> {
    private val parser = JsonParser()
    override fun applyEx(json: JsonObject, stopString: String?) =
        json.apply {
            val stop = parser.parse(stopString).asJsonObject
            getAsJsonObject("vehicle").apply {
                add("stop", stop)
                remove("stopId")
            }
        }
}