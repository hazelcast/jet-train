package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.FunctionEx
import java.util.*

class RemoveFirstAndLastChars : FunctionEx<String, String> {
    override fun applyEx(line: String) = line.substring(1, line.length - 1)
}

class SplitByDoubleQuotes : FunctionEx<String, List<String>> {
    override fun applyEx(line: String) = line.split("\",\"")
}

class RemoveDoubleQuotes : FunctionEx<List<String>, List<String>> {
    override fun applyEx(words: List<String>) = words.map { it.replace("\"", "") }
}

class ToEntry :
    FunctionEx<JsonObject?, Map.Entry<Any, JsonObject>> {
    override fun applyEx(json: JsonObject?): Map.Entry<Any, JsonObject>? {
        return if (json != null) {
            val id = json.get("id")
            if (id.isString) AbstractMap.SimpleEntry(id.asString(), json)
            else AbstractMap.SimpleEntry(id.asObject(), json)
        } else null
    }
}