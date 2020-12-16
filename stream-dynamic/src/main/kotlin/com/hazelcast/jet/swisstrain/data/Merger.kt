package com.hazelcast.jet.swisstrain.data

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.map.IMap
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

object TripIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject): String? {
        val tripId: String? = json
            .getAsJsonObject("vehicle")
            ?.getAsJsonObject("trip")
            ?.getAsJsonPrimitive("tripId")
            ?.asString
        return if (tripId == null) null
        else "${json.getAsJsonPrimitive("agencyId").asString}:$tripId"
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

// Magic number
// This is the highest sequence count of stops for a trip in regard to the current static data set
private const val MAX_SEQ_NUMBER = 69

object MergeWithStopTimes :
    BiFunctionEx<IMap<com.hazelcast.internal.json.JsonObject, com.hazelcast.internal.json.JsonObject>, com.hazelcast.internal.json.JsonObject, com.hazelcast.internal.json.JsonObject> {

    private const val pattern = "HH:mm:ss"

    override fun applyEx(
        stopTimes: IMap<com.hazelcast.internal.json.JsonObject, com.hazelcast.internal.json.JsonObject>,
        tripUpdate: com.hazelcast.internal.json.JsonObject
    ): com.hazelcast.internal.json.JsonObject? {
        val tripId = tripUpdate.getString("id", null)
        val updatedStopTimes = IntRange(1, MAX_SEQ_NUMBER)
            .mapNotNull { findStopTime(tripId, it, stopTimes) }
            .map { adjustStopTime(tripUpdate, it) }
            .mapNotNull {
                // Cleanup the structure while we are in the JSON model
                com.hazelcast.internal.json.JsonObject(it).remove("id").remove("trip")
            }.fold(JsonArray()) { accumulator, current ->
                accumulator.add(current)
            }
        return if (!updatedStopTimes.isEmpty)
            com.hazelcast.internal.json.JsonObject(tripUpdate)
                .set("schedule", updatedStopTimes)
                .remove("trip_update")
        else null
    }

    private fun adjustStopTime(
        tripUpdate: com.hazelcast.internal.json.JsonObject,
        stopTime: com.hazelcast.internal.json.JsonObject
    ): com.hazelcast.internal.json.JsonObject {
        val stopTimeUpdates = tripUpdate["trip_update"]
            ?.asObject()
            ?.get("stop_time_update")
            ?.asArray()
        return adjust(stopTimeUpdates, stopTime)
    }

    private fun findStopTime(
        tripId: String?,
        sequence: Int,
        stopTimes: IMap<com.hazelcast.internal.json.JsonObject, com.hazelcast.internal.json.JsonObject>
    ): com.hazelcast.internal.json.JsonObject? {
        val stopTimeId = com.hazelcast.internal.json.JsonObject()
            .set("trip", tripId)
            .set("sequence", sequence)
        return stopTimes[stopTimeId]
    }

    /**
     * @param stopTimeUpdates Dynamic data, of the following form:
     *
     * [{
     *     "stop_sequence": 1,
     *     "departure": {
     *       "delay": 0
     *     },
     *     "stop_id": "8593207",
     *     "schedule_relationship": "SCHEDULED"
     *   },
     *   {
     *     "stop_sequence": 7,
     *     "arrival": {
     *       "delay": -60
     *     },
     *     "departure": {
     *       "delay": -60
     *     },
     *   "stop_id": "8593195",
     *   "schedule_relationship": "SCHEDULED"
     * }]
     *
     * @param stopTime Reference data, of the following form:
     *
     * {
     *   "id" : {
     *     "trip" : "4.TA.16-622-j20-1.3.H",
     *     "sequence" : 12
     *   },
     *   "trip" : "4.TA.16-622-j20-1.3.H",
     *   "arrival" : "06:14:00",
     *   "departure" : "06:14:00",
     *   "stop" : "8578537",
     *   "sequence" : 12
     * }
     */
    private fun adjust(
        stopTimeUpdates: com.hazelcast.internal.json.JsonArray?,
        stopTime: com.hazelcast.internal.json.JsonObject
    ): com.hazelcast.internal.json.JsonObject {
        val sequence = stopTime.getInt("sequence", -1)
        val matchingUpdate = stopTimeUpdates
            ?.map { it as com.hazelcast.internal.json.JsonObject }
            ?.firstOrNull {
                it.getInt("stop_sequence", -1) == sequence
            }
        return if (matchingUpdate == null) stopTime
        else com.hazelcast.internal.json.JsonObject(stopTime).apply {
            adjustDelayIfNecessary(this, matchingUpdate, "arrival")
            adjustDelayIfNecessary(this, matchingUpdate, "departure")
        }
    }

    private fun adjustDelayIfNecessary(
        stopTime: com.hazelcast.internal.json.JsonObject,
        matchingUpdate: com.hazelcast.internal.json.JsonObject,
        what: String
    ) {
        val update =
            matchingUpdate[what] as com.hazelcast.internal.json.JsonObject? ?: com.hazelcast.internal.json.JsonObject()
        val delay = update.getInt("delay", 0)
        val timeString = stopTime.getString(what, null)
        if (timeString != null) {
            val formatter = DateTimeFormatter
                .ofPattern(pattern)
                .withResolverStyle(ResolverStyle.LENIENT)
            val originalTime = LocalTime.from(formatter.parse(timeString))
            val adjustedTime = originalTime + Duration.ofSeconds(delay.toLong())
            stopTime.set(what, formatter.format(adjustedTime))
        }
    }
}

object MergeWithLocation :
    BiFunctionEx<IMap<String, com.hazelcast.internal.json.JsonObject>, com.hazelcast.internal.json.JsonObject, com.hazelcast.internal.json.JsonObject> {
    override fun applyEx(
        stops: IMap<String, com.hazelcast.internal.json.JsonObject>,
        json: com.hazelcast.internal.json.JsonObject
    ): com.hazelcast.internal.json.JsonObject {
        val schedule = json
            .get("schedule")
            .asArray()
            .map { it as com.hazelcast.internal.json.JsonObject }
            .mapNotNull {
                val stopId = it.getString("stop", null)
                val stop = stops[stopId]
                if (stop != null) {
                    com.hazelcast.internal.json.JsonObject(it)
                        .set("stop", stop.getString("stop_name", null))
                        .set("longitude", stop.getString("stop_long", null))
                        .set("latitude", stop.getString("stop_lat", null))
                } else null
            }.fold(com.hazelcast.internal.json.JsonArray()) { accumulator, current ->
                accumulator.add(current)
            }
        return com.hazelcast.internal.json.JsonObject(json).set("schedule", schedule)
    }
}