package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.FunctionEx

sealed class ToJson(private val mappings: Map<String, Int>) : FunctionEx<List<String>, JsonObject?> {
    override fun applyEx(list: List<String>) =
        if (list.size > mappings.size)
            JsonObject().apply {
                mappings.forEach {
                    val value = list[it.value]
                    set(it.key, value)
                }
            }
        else null
}

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

class ToStopTime : FunctionEx<List<String>, JsonObject?> {
    override fun applyEx(t: List<String>): JsonObject? =
        if (t.size > 4)
            JsonObject().apply {
                set("trip", t[0])
                set("arrival", t[1])
                set("departure", t[2])
                set("stop", t[3])
                set("sequence", t[4])
                set("id", JsonObject().add("trip", t[0]).add("stop", t[3]))
            } else null
}