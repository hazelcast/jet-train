package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.FunctionEx

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
        "stop_lat" to 2,
        "stop_long" to 3
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
        "route_type" to 4
    )
)

object ToTrip : ToJson(
    mapOf(
        "route_id" to 0,
        "id" to 2,
        "trip_headsign" to 3
    )
)

object ToStopTime : FunctionEx<List<String>, JsonObject?> {
    override fun applyEx(t: List<String>) =
        if (t.size > 4) JsonObject()
            .apply {
                set("trip", t[0])
                set("arrival", t[1])
                set("departure", t[2])
                set("stop", t[3])
                set("sequence", t[4].toInt())
                set("id", JsonObject().add("trip", t[0]).add("sequence", t[4].toInt()))
            }
        else null
}