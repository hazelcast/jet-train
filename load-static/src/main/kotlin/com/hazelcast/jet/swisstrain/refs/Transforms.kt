package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.Json
import com.hazelcast.jet.Util
import java.io.Serializable

object ToEntry : FunctionEx<String?, Map.Entry<Serializable, String>> {
    override fun applyEx(string: String?): Map.Entry<Serializable, String>? {
        return if (string == null) null
        else {
            val id = Json.parse(string)?.asObject()?.get("id")
            when {
                id == null -> null
                id.isString -> Util.entry(id.asString(), string)
                else -> Util.entry(id.asObject(), string)
            }
        }
    }
}