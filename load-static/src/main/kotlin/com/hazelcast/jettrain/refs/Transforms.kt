package com.hazelcast.jettrain.refs

import com.hazelcast.core.HazelcastJsonValue
import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.Json
import com.hazelcast.internal.json.JsonValue
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.Util
import java.io.Serializable

object ToEntry : FunctionEx<String?, Map.Entry<Serializable, Any>> {
    override fun applyEx(string: String?): Map.Entry<Serializable, Any>? {
        val id: JsonValue? = string?.let(Json::parse)
            ?.asObject()
            ?.get("id")
        return when (id?.isString) {
            null -> null
            true -> Util.entry(id.asString(), string)
            false -> Util.entry(id.toString(), HazelcastJsonValue(string))
        }
    }
}

object MapTraverser : FunctionEx<Map<String, List<String>>, Traverser<Pair<String, List<String>>>> {
    override fun applyEx(map: Map<String, List<String>>): Traverser<Pair<String, List<String>>> {
        val iterator = map.iterator()
        return Traverser {
            if (iterator.hasNext()) iterator.next().toPair()
            else null
        }
    }
}