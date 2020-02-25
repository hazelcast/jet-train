package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.FunctionEx
import java.util.*

object RemoveFirstAndLastChars : FunctionEx<String, String> {
    override fun applyEx(line: String) = line.substring(1, line.length - 1)
}

object SplitByDoubleQuotes : FunctionEx<String, List<String>> {
    override fun applyEx(line: String) = line.split("\",\"")
}

object RemoveDoubleQuotes : FunctionEx<List<String>, List<String>> {
    override fun applyEx(words: List<String>) = words.map { it.replace("\"", "") }
}

object ToEntry : FunctionEx<JsonObject?, Map.Entry<Any, JsonObject>> {
    override fun applyEx(json: JsonObject?) =
        if (json != null) {
            val id = json.get("id")
            if (id.isString) AbstractMap.SimpleEntry(id.asString(), json)
            else AbstractMap.SimpleEntry(id.asObject(), json)
        } else null
}