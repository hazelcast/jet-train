package com.hazelcast.jettrain.data

import com.google.gson.*
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

/**
 * Flatten the JSON structure so that the front-end JavaScript doesn't need to do it.
 *
 * @param json <em>e.g.</em>
 * <pre><code>
 * {
 *     "agencyId": "AC",
 *     "id": "10",
 *     "schedule": [ ... ],
 *     "stop": { ... },
 *     "vehicle": {
 *         "currentStatus": "IN_TRANSIT_TO",
 *         "currentStopSequence": 4,
 *         "occupancyStatus": "FEW_SEATS_AVAILABLE",
 *         "position": {
 *             "bearing": 178.0,
 *             "latitude": 37.83096,
 *             "longitude": -122.293015,
 *             "speed": 8.9408
 *         },
 *         "stopId": "52257",
 *         "timestamp": "1614327812",
 *         "trip": {
 *             "route": {
 *                 "agency_id": "AC",
 *                 "id": "AC:F",
 *                 "route_color": "FFA500",
 *                 "route_name": "F",
 *                 "route_type": "3"
 *             },
 *             "scheduleRelationship": "SCHEDULED",
 *             "tripId": "11857020",
 *             "trip_headsign": "U.C. Campus"
 *         },
 *         "vehicle": {
 *             "id": "6152",
 *             "label": "",
 *             "licensePlate": ""
 *         }
 *     }
 * }</code></pre>
 *
 * @return A Json object with properties directly accessible at the root-level
 * <pre><code>
 *  {
 *     "agencyId": "AC",
 *     "agencyName": "AC",
 *     "id": "10",
 *     "position": {
 *         "bearing": 178.0,
 *         "latitude": 37.83096,
 *         "longitude": -122.293015,
 *         "speed": 8.9408
 *     },
 *     "routeColor": "FFA500",
 *     "routeId": "AC:F",
 *     "routeName": "F",
 *     "routeType": "3",
 *     "schedule": [ ... ],
 *     "stop": { ... },
 *     "vehicle": {
 *         "currentStatus": "IN_TRANSIT_TO",
 *         "currentStopSequence": 4,
 *         "occupancyStatus": "FEW_SEATS_AVAILABLE",
 *         "position": {
 *             "bearing": 178.0,
 *             "latitude": 37.83096,
 *             "longitude": -122.293015,
 *             "speed": 8.9408
 *         },
 *         "stopId": "52257",
 *         "timestamp": "1614327812",
 *         "trip": {
 *             "route": {
 *                 "agency_id": "AC",
 *                 "id": "AC:F",
 *                 "route_color": "FFA500",
 *                 "route_name": "F",
 *                 "route_type": "3"
 *             },
 *             "scheduleRelationship": "SCHEDULED",
 *             "tripId": "11857020",
 *             "trip_headsign": "U.C. Campus"
 *         },
 *         "vehicle": {
 *             "id": "6152",
 *             "label": "",
 *             "licensePlate": ""
 *         }
 *     },
 *     "vehicleId": "6152"
 * }</code></pre>
 */
object ToFlattenedStructure : FunctionEx<JsonObject, JsonObject> {
    override fun applyEx(json: JsonObject): JsonObject {
        val vehicle = json.getAsJsonObject("vehicle")
        val trip = vehicle.getAsJsonObject("trip")
        val route = trip.getAsJsonObject("route")
        val innerVehicle = vehicle.getAsJsonObject("vehicle")
        return Gson().fromJson(json, JsonObject::class.java).apply {
            add("vehicleId", innerVehicle.getAsJsonPrimitive("id"))
            add("routeId", route.getAsJsonPrimitive("id"))
            add("routeName", route.getAsJsonPrimitive("route_name"))
            add("routeType", route.getAsJsonPrimitive("route_type"))
            add("routeType", route.getAsJsonPrimitive("route_type"))
            add("routeColor", route.getAsJsonPrimitive("route_color"))
            add("agencyName", json.getAsJsonPrimitive("agencyId"))
            add("position", vehicle.getAsJsonObject("position"))
        }
    }
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