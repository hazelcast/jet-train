package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonValue
import com.hazelcast.jet.Util
import java.io.Serializable

object ToEntry : FunctionEx<String?, Map.Entry<Serializable, String>> {
    override fun applyEx(string: String?): Map.Entry<Serializable, String>? {
        val id: JsonValue? = string?.let(Json::parse)
            ?.asObject()
            ?.get("id")
        return when (id?.isString) {
            null -> null
            true -> Util.entry(id.asString(), string)
            false -> Util.entry(id.asObject(), string)
        }
    }
}