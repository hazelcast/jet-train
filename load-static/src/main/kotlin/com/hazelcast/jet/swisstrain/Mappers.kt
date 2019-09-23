package com.hazelcast.jet.swisstrain

import org.json.JSONObject

internal fun toStop(list: List<String>) =
    toJson(
        list,
        mapOf(
            "id" to 0,
            "name" to 1,
            "lat" to 2,
            "long" to 3
        ),
        mapOf(
            "lat" to String::toDouble,
            "long" to String::toDouble
        )
    )

internal fun toAgency(list: List<String>) =
    toJson(
        list,
        mapOf(
            "id" to 0,
            "name" to 1
        )
    )

internal fun toRoute(list: List<String>) =
    toJson(
        list,
        mapOf(
            "id" to 0,
            "agency" to 1,
            "name" to 2,
            "description" to 4,
            "type" to 5
        ),
        mapOf("type" to String::toInt)
    )

internal fun toTrip(list: List<String>) =
    toJson(
        list,
        mapOf(
            "route" to 0,
            "service" to 1,
            "id" to 2,
            "headsign" to 3,
            "shortname" to 4
        )
    )

private fun toJson(
    list: List<String>,
    mappings: Map<String, Int>,
    transforms: Map<String, (String) -> Any> = mapOf()
): JSONObject? = if (list.size > mappings.size) {
    JSONObject().apply {
        mappings.forEach {
            val value = list[it.value]
            val transform = transforms[it.key]
            put(it.key, if (transform != null) transform(value) else value)
        }
    }
} else null

