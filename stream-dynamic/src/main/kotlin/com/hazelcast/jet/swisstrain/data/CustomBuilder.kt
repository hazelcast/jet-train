package com.hazelcast.jet.swisstrain.data

import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.function.BiConsumerEx
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.SourceBuilder
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer
import java.io.Serializable
import java.time.Instant

private const val PATTERN = "{\"id\":\""

fun remoteService(url: String, token: String) = SourceBuilder
    .stream("http-source", CreateContext(url))
    .fillBufferFn<String>(FillBuffer(token))
    .build()

class FillBuffer(private val token: String) : BiConsumerEx<Pair<String, TimeHolder>, SourceBuffer<String>> {
    override fun acceptEx(tuple: Pair<String, TimeHolder>, buffer: SourceBuffer<String>) {
        if (Instant.now().isAfter(tuple.second.value.plusSeconds(30))) {
            val (_, _, result) =
                Fuel.get(tuple.first).header("Authorization" to token).responseString()
            when (result) {
                is Result.Failure -> println(result.getException())
                is Result.Success -> buffer.add(result.get())
            }
            tuple.second.reset()
        }
    }
}

class CreateContext(private val url: String) : FunctionEx<Processor.Context, Pair<String, TimeHolder>> {
    override fun applyEx(ctx: Processor.Context): Pair<String, TimeHolder> {
        return url to TimeHolder()
    }
}

class TimeHolder(var value: Instant = Instant.now().minusSeconds(31)) : Serializable {
    fun reset() {
        value = Instant.now()
    }
}