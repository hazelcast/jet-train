package com.hazelcast.jettrain.refs

import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.JsonArray
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Util
import java.io.Serializable

class ToNameValuePair(private val fields: List<String>) : FunctionEx<Array<String>, List<Pair<String, String>>> {
    override fun applyEx(values: Array<String>) = fields.zip(values)
}

object ToJson : FunctionEx<List<Pair<String, String>>, JsonObject> {
    private val floatColumns = listOf("stop_long", "stop_lat")
    override fun applyEx(pairs: List<Pair<String, String>>) =
        pairs.fold(JsonObject()) { json, pair ->
            if (floatColumns.contains(pair.first)) json.add(pair.first, pair.second.toFloat())
            else json.add(pair.first, pair.second)
            json
        }
}

object ToEntry : FunctionEx<JsonObject, Map.Entry<Serializable, String>> {
    override fun applyEx(json: JsonObject?): Map.Entry<Serializable, String>? {
        val id = json?.getString("id", null)
        return if (id != null) Util.entry(id, json.toString())
        else null
    }
}

object ToEntryForStopTime : FunctionEx<JsonObject, Map.Entry<String, String>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, String> = Util.entry(
        json.get("id").asString(),
        json.get("schedule").asArray().toString()
    )
}

object ToStopTime : FunctionEx<Map.Entry<String, List<Array<String>>>, JsonObject> {
    override fun applyEx(entry: Map.Entry<String, List<Array<String>>>): JsonObject {
        val wrapper = JsonObject().add("id", entry.key)
        val schedule = entry.value.fold(JsonArray()) { array, element ->
            val json = JsonObject().apply {
                if (element.size > 1) add("departure", element[1])
                if (element.size > 2) add("arrival", element[2])
                if (element.size > 3) add("stopId", element[3])
                if (element.size > 4) add("sequence", element[4])
            }
            array.add(json)
        }
        return wrapper.add("schedule", schedule)
    }
}