package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonObject

sealed class ToJson(private val mappings: Map<String, Int>) : FunctionEx<List<String>, String?> {
    override fun applyEx(list: List<String>) =
        if (list.size > mappings.size) JsonObject()
            .apply {
                mappings.forEach {
                    val value = list[it.value]
                    set(it.key, value)
                }
            }.toString()
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
        "trip_headsign" to 3
    )
)