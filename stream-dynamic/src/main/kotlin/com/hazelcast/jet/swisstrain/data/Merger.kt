package com.hazelcast.jet.swisstrain.data

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.internal.json.JsonValue
import com.hazelcast.jet.JetInstance
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx

class ContextCreator : FunctionEx<JetInstance, HazelcastInstance> {
    override fun applyEx(jet: JetInstance) =
        jet.hazelcastInstance
}

class MergeWithTrip :
    BiFunctionEx<HazelcastInstance, JsonObject, JsonObject> {

    override fun applyEx(instance: HazelcastInstance, json: JsonObject): JsonObject {
        val tripId = json.getString("id", null)
        instance.getMap<String, JsonObject>("trips")[tripId]
            ?.let {
                return json.merge(it)
            }
        return json
    }
}

class MergeWithStops :
    BiFunctionEx<HazelcastInstance, JsonObject, JsonObject> {

    override fun applyEx(instance: HazelcastInstance, json: JsonObject): JsonObject {
        val stopTimes = instance.getMap<JsonObject, JsonObject>("stop_times")
        val stops = instance.getMap<String, JsonObject>("stops")
        val tripId = json.getString("id", null)
        json["trip_update"]?.asObject()
            ?.get("stop_time_update")?.asArray()
            ?.forEach {
                enrichStopTimeUpdates(it, tripId, stopTimes, stops)
            }
        return json
    }

    private fun enrichStopTimeUpdates(
        it: JsonValue,
        tripId: String,
        stopTimes: IMap<JsonObject, JsonObject>,
        stops: IMap<String, JsonObject>
    ) {
        val stopTimeUpdate = it as JsonObject
        val stopId = stopTimeUpdate.getString("stop_id", null)
        val stopTimeId = JsonObject().add("trip", tripId).add("stop", stopId)
        stopTimes[stopTimeId]?.let { stopTime ->
            stopTimeUpdate
                .set("stop_time_arrival", stopTime.get("arrival"))
                .set("stop_time_departure", stopTime.get("departure"))
        }
        stops[stopId]?.let { stop ->
            stopTimeUpdate
                .set("stop_lat", stop.get("stop_lat"))
                .set("stop_long", stop.get("stop_long"))
                .set("stop_name", stop.get("stop_name"))
        }
    }
}

