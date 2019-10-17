package com.hazelcast.jet.swisstrain.data

import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.function.FunctionEx
import java.io.Serializable
import java.time.LocalTime
import java.util.*

class SplitPayload : FunctionEx<String, Traverser<JsonObject>> {
    override fun applyEx(payload: String): Traverser<JsonObject> {
        val json = Json.parse(payload).asObject()
        val entities = json.get("entity").asArray()
        return TripTraverser(entities)
    }
}

class TripTraverser(entities: JsonArray) : Traverser<JsonObject>, Serializable {
    private val iterator = entities.iterator()
    override fun next() =
        if (iterator.hasNext()) iterator.next().asObject()
        else null
}

class AdjustTimeWithDelays : FunctionEx<JsonObject, JsonObject> {
    override fun applyEx(json: JsonObject): JsonObject {
        json["trip_update"]?.asObject()
            ?.get("stop_time_update")?.asArray()
            ?.map { it.asObject() }
            ?.forEach {
                it.let { stopTimeUpdate ->
                    computeTime(stopTimeUpdate, "departure")
                    computeTime(stopTimeUpdate, "arrival")
                }
            }
        return json
    }

    private fun computeTime(stopTimeUpdate: JsonObject, name: String) {
        stopTimeUpdate[name]?.asObject()?.get("delay")?.let {
            val delay = it.asLong()
            val stopTimeString = stopTimeUpdate.getString("stop_time_$name", null)
            if (stopTimeString != null) {
                val time = LocalTime.parse(stopTimeString)
                val newTime = time.plusSeconds(delay)
                stopTimeUpdate.set("${name}_time", newTime.toString())
            }
        }
    }
}

class ComputeLocation : FunctionEx<JsonObject, JsonObject> {
    override fun applyEx(json: JsonObject): JsonObject? {
        val stopTimeUpdates = json["trip_update"]?.asObject()
            ?.get("stop_time_update")?.asArray()
        curate(stopTimeUpdates)
        return when {
            stopTimeUpdates == null -> null
            stopTimeUpdates.isEmpty -> null
            stopTimeUpdates.size() == 1 -> null
            stopTimeUpdates.size() == 2 -> toMoving(stopTimeUpdates, 0, 1)
            else -> {
                val computedStops = findStops(stopTimeUpdates)
                return json.merge(computedStops)
                    .remove("trip_update")
            }
        }
    }

    // Because sometimes, intermediate stops have neither arrival nor departure
    // And yes, it's ugly how it's handled, but I have no other ideas right now
    // PR welcome
    private fun curate(stopTimeUpdates: JsonArray?) {
        val badIndices = mutableListOf<Int>()
        stopTimeUpdates?.forEachIndexed { index, item ->
            val stopTimeUpdate = item.asObject()
            if (!isDataSafe(stopTimeUpdate)) {
                badIndices.add(index)
            }
        }
        badIndices.reversed().forEach {
            stopTimeUpdates?.remove(it)
        }
    }

    private fun findStops(stopTimeUpdates: JsonArray): JsonObject {
        val now = LocalTime.now()
        val bound = stopTimeUpdates.size() - 1
        var previous: JsonObject
        var next: JsonObject
        for (i in 0 until bound) {
            previous = stopTimeUpdates.get(i).asObject()
            next = stopTimeUpdates.get(i + 1).asObject()
            val previousArrival = arrivalTime(previous)
            val previousDeparture = departureTime(previous)
            val nextArrival = arrivalTime(next)
            if (now.isBefore(previousDeparture) && now.isAfter(previousArrival)) {
                return toStopped(stopTimeUpdates, i, i + 1)
            }
            if (now.isAfter(previousDeparture) && now.isBefore(nextArrival)) {
                return toMoving(stopTimeUpdates, i, i + 1)
            }
        }
        return toStopped(stopTimeUpdates, bound - 1, bound)
    }

    private fun arrivalTime(stopTimeUpdate: JsonObject) =
        time(stopTimeUpdate, "arrival_time")

    private fun departureTime(stopTimeUpdate: JsonObject) =
        time(stopTimeUpdate, "departure_time")

    private fun time(stopTimeUpdate: JsonObject, which: String): LocalTime? {
        val timeString = stopTimeUpdate.getString(which, null)
        return if (timeString == null) null
        else LocalTime.parse(timeString)
    }

    private fun isDataSafe(json: JsonObject) =
        json.get("arrival_time") != null || json.get("arrival_time") != null

    private fun toStopped(stopTimeUpdates: JsonArray, from: Int, to: Int) =
        toStatus(stopTimeUpdates, from, to, "STOPPED")

    private fun toMoving(stopTimeUpdates: JsonArray, from: Int, to: Int) =
        toStatus(stopTimeUpdates, from, to, "MOVING")

    private fun toStatus(stopTimeUpdates: JsonArray, from: Int, to: Int, status: String) =
        JsonObject().add("from", stopTimeUpdates.get(from))
            .add("to", stopTimeUpdates.get(to))
            .add("status", status)
}

class ToEntry : FunctionEx<JsonObject, Map.Entry<String, JsonObject>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, JsonObject> =
        AbstractMap.SimpleEntry(json.getString("id", null), json)
}