package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonObject

sealed class ToJson(private val mappings: Map<String, Int>) : FunctionEx<List<String>, JsonObject?> {
    override fun applyEx(list: List<String>) =
        if (list.size > mappings.size) JsonObject()
            .apply {
                mappings.forEach {
                    val value = list[it.value]
                    set(it.key, value)
                }
            }
        else null
}

object ToStop : ToJson(
    mapOf(
        "id" to 0,
        "stop_name" to 1,
        "stop_lat" to 4,
        "stop_long" to 5
    )
)

object ToAgency : ToJson(
    mapOf(
        "id" to 0,
        "agency_name" to 1
    )
)

object ToRoute : ToJson(
    mapOf(
        "id" to 0,
        "agency_id" to 1,
        "route_name" to 2,
        "route_type" to 5
    )
)

object ToTrip : ToJson(
    mapOf(
        "route_id" to 0,
        "id" to 2,
        "trip_headsign" to 2
    )
)

object ToStopTime : FunctionEx<List<String>, JsonObject?> {
    override fun applyEx(t: List<String>) =
        if (t.size > 4) JsonObject()
            .apply {
                val seq = t[4].toIntOrNull() ?: 0 // Handle the case of the first line
                set("trip", t[0])
                set("arrival", t[1])
                set("departure", t[2])
                set("stop", t[3])
                set("sequence", seq)
                set("id", JsonObject().add("trip", t[0]).add("sequence", seq))
            }
        else null
}