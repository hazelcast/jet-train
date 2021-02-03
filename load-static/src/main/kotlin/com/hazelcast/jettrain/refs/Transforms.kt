package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Util
import java.io.Serializable

object ToEntry : FunctionEx<String?, Map.Entry<Serializable, Any>> {
    override fun applyEx(string: String?): Map.Entry<Serializable, Any>? {
        val id = string?.let(Json::parse)
            ?.asObject()
            ?.getString("id", null)
        return if (id != null) Util.entry(id, string)
        else null
    }
}

object ToEntryForStopTime : FunctionEx<JsonObject, Map.Entry<String, String>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, String> = Util.entry(
        json.get("id").asString(),
        json.get("schedule").asArray().toString()
    )
}