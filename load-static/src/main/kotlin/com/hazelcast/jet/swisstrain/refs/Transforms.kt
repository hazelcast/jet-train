package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonObject
import java.util.*

object ToEntry : FunctionEx<JsonObject?, Map.Entry<Any, JsonObject>> {
    override fun applyEx(json: JsonObject?) =
        if (json != null) {
            val id = json.get("id")
            if (id.isString) AbstractMap.SimpleEntry(id.asString(), json)
            else AbstractMap.SimpleEntry(id.asObject(), json)
        } else null
}