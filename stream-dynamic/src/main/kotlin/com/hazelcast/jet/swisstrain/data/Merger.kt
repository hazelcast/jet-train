package com.hazelcast.jet.swisstrain.data

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.JetInstance
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

object ContextCreator : FunctionEx<JetInstance, HazelcastInstance> {
    override fun applyEx(jet: JetInstance) = jet.hazelcastInstance
}

object TripIdExtractor : FunctionEx<JsonObject, String?> {
    override fun applyEx(json: JsonObject): String? = json.getString("id", null)
}

object MergeWithTrip : BiFunctionEx<JsonObject, JsonObject?, JsonObject> {
    override fun applyEx(json: JsonObject, trip: JsonObject?): JsonObject =
        if (trip == null) json
        else json.merge(trip)
}

// Magic number
// This is the highest sequence count of stops for a trip in regard to the current static data set
private const val MAX_SEQ_NUMBER = 69

object MergeWithStopTimes :
    BiFunctionEx<HazelcastInstance, JsonObject, JsonObject> {

    private val pattern = "HH:mm:ss"

    override fun applyEx(instance: HazelcastInstance, tripUpdate: JsonObject): JsonObject? {
        val stopTimes = instance.getMap<JsonObject, JsonObject>("stop_times")
        val tripId = tripUpdate.getString("id", null)
        val updatedStopTimes = IntRange(1, MAX_SEQ_NUMBER)
            .mapNotNull { findStopTime(tripId, it, stopTimes) }
            .map { adjustStopTime(tripUpdate, it) }
            .mapNotNull {
                // Cleanup the structure while we are in the JSON model
                JsonObject(it).remove("id").remove("trip")
            }.fold(JsonArray()) { accumulator, current ->
                accumulator.add(current)
            }
        return if (!updatedStopTimes.isEmpty)
            JsonObject(tripUpdate)
                .set("schedule", updatedStopTimes)
                .remove("trip_update")
        else null
    }

    private fun adjustStopTime(tripUpdate: JsonObject, stopTime: JsonObject): JsonObject {
        val stopTimeUpdates = tripUpdate["trip_update"]
            ?.asObject()
            ?.get("stop_time_update")
            ?.asArray()
        return adjust(stopTimeUpdates, stopTime)
    }

    private fun findStopTime(tripId: String?, sequence: Int, stopTimes: IMap<JsonObject, JsonObject>): JsonObject? {
        val stopTimeId = JsonObject()
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
    private fun adjust(stopTimeUpdates: JsonArray?, stopTime: JsonObject): JsonObject {
        val sequence = stopTime.getInt("sequence", -1).toInt()
        val matchingUpdate = stopTimeUpdates
            ?.map { it as JsonObject }
            ?.firstOrNull {
                it.getInt("stop_sequence", -1) == sequence
            }
        return if (matchingUpdate == null) stopTime
        else JsonObject(stopTime).apply {
            adjustDelayIfNecessary(this, matchingUpdate, "arrival")
            adjustDelayIfNecessary(this, matchingUpdate, "departure")
        }
    }

    private fun adjustDelayIfNecessary(stopTime: JsonObject, matchingUpdate: JsonObject, what: String) {
        val update = matchingUpdate[what] as JsonObject? ?: JsonObject()
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

object MergeWithLocation : BiFunctionEx<HazelcastInstance, JsonObject, JsonObject> {
    override fun applyEx(instance: HazelcastInstance, json: JsonObject): JsonObject {
        val stops = instance.getMap<String, JsonObject>("stops")
        val schedule = json
            .get("schedule")
            .asArray()
            .map { it as JsonObject }
            .mapNotNull {
                val stopId = it.getString("stop", null)
                val stop = stops[stopId]
                if (stop != null) {
                    JsonObject(it)
                        .set("stop", stop.getString("stop_name", null))
                        .set("longitude", stop.getString("stop_long", null))
                        .set("latitude", stop.getString("stop_lat", null))
                } else null
            }.fold(JsonArray()) { accumulator, current ->
                accumulator.add(current)
            }
        return JsonObject(json).set("schedule", schedule)
    }
}