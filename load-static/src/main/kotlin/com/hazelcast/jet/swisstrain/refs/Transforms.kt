package com.hazelcast.jet.swisstrain.refs

import java.util.*
import org.json.JSONObject
import com.hazelcast.jet.function.FunctionEx

class RemoveFirstAndLastChars : FunctionEx<String, String> {
    override fun applyEx(line: String) = line.substring(1, line.length - 1)
}

class SplitByDoubleQuotes : FunctionEx<String, List<String>> {
    override fun applyEx(line: String) = line.split("\",\"")
}

class RemoveDoubleQuotes : FunctionEx<List<String>, List<String>> {
    override fun applyEx(words: List<String>) = words.map { it.replace("\"", "") }
}

class ToEntry(private val withToJson: FunctionEx<List<String>, JSONObject?>) :
    FunctionEx<List<String>, Map.Entry<String, String>> {
    override fun applyEx(line: List<String>): Map.Entry<String, String>? {
        val json = withToJson.applyEx(line)
        return if (json != null) AbstractMap.SimpleEntry(json.getString("id"), json.toString())
        else null
    }
}