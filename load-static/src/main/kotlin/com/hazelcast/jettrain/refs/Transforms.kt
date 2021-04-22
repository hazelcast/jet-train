package com.hazelcast.jettrain.refs

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.Util
import java.io.Serializable

class ToNameValuePair(private val fields: List<String>) : FunctionEx<Array<String>, List<Pair<String, String>>> {
    override fun applyEx(values: Array<String>) = fields.zip(values)
}

object ToJson : FunctionEx<List<Pair<String, String>>, JsonObject> {
    private val floatColumns = listOf("stop_long", "stop_lat")
    override fun applyEx(pairs: List<Pair<String, String>>) =
        pairs.fold(JsonObject()) { json, pair ->
            if (floatColumns.contains(pair.first)) json.addProperty(pair.first, pair.second.toFloat())
            else json.addProperty(pair.first, pair.second)
            json
        }
}

object ToEntry : FunctionEx<JsonObject, Map.Entry<Serializable, String>> {
    override fun applyEx(json: JsonObject?): Map.Entry<Serializable, String>? {
        val id = json?.getAsJsonPrimitive("id")?.asString
        return if (id != null) Util.entry(id, json.toString())
        else null
    }
}

object ToEntryForStopTime : FunctionEx<JsonObject, Map.Entry<String, String>> {
    override fun applyEx(json: JsonObject): Map.Entry<String, String> = Util.entry(
        json.getAsJsonPrimitive("id").asString,
        json.getAsJsonArray("schedule").toString()
    )
}

object ToStopTime : FunctionEx<Map.Entry<String, List<Array<String>>>, JsonObject> {
    override fun applyEx(entry: Map.Entry<String, List<Array<String>>>): JsonObject {
        val wrapper = JsonObject().apply { addProperty("id", entry.key) }
        val schedule = entry.value.fold(JsonArray()) { array, element ->
            val json = JsonObject().apply {
                if (element.size > 1) addProperty("departure", element[1])
                if (element.size > 2) addProperty("arrival", element[2])
                if (element.size > 3) addProperty("stopId", element[3])
                if (element.size > 4) addProperty("sequence", element[4])
            }
            array.add(json)
            array
        }
        return wrapper.apply { add("schedule", schedule) }
    }
}