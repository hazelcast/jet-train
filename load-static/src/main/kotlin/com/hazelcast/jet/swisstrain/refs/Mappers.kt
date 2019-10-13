package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.function.FunctionEx
import org.json.JSONObject

class ToStop : ToJson(
    mapOf(
        "id" to 0,
        "name" to 1,
        "lat" to 2,
        "long" to 3
    )
)

class ToAgency : ToJson(
    mapOf(
        "id" to 0,
        "name" to 1
    )
)

class ToRoute : ToJson(
    mapOf(
        "id" to 0,
        "agency" to 1,
        "name" to 2,
        "description" to 4,
        "type" to 5
    )
)

class ToTrip : ToJson(
    mapOf(
        "route" to 0,
        "service" to 1,
        "id" to 2,
        "headsign" to 3,
        "shortname" to 4
    )
)

class ToStopTime : ToJson(
    mapOf(
        "tripId" to 0,
        "arrival" to 1,
        "departure" to 2,
        "id" to 3,
        "sequence" to 4
    )
)

sealed class ToJson(private val mappings: Map<String, Int>) : FunctionEx<List<String>, JSONObject?> {
    override fun applyEx(list: List<String>) =
        if (list.size > mappings.size)
            JSONObject().apply {
                mappings.forEach {
                    val value = list[it.value]
                    put(it.key, value)
                }
            }
        else null
}