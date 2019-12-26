package com.hazelcast.jet.swisstrain.data

import java.io.Serializable
import java.time.Instant
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.function.BiConsumerEx
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.SourceBuilder
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer

fun remoteService(url: String, token: String) = SourceBuilder
    .batch("http-source", CreateContext())
    .fillBufferFn(FillBuffer(url, token))
    .build()

class FillBuffer(private val url: String, private val token: String) : BiConsumerEx<TimeHolder, SourceBuffer<String>> {
    override fun acceptEx(time: TimeHolder, buffer: SourceBuffer<String>) {
        if (Instant.now().isAfter(time.value.plusSeconds(30))) {
            val (_, _, result) =
                Fuel.get(url).header(
                    "Authorization" to token,
                    "Accept-Encoding" to "gzip, deflate").responseString()
            when (result) {
                is Result.Failure -> println(result.getException())
                is Result.Success -> buffer.add(result.get())
            }
            time.reset()
        }
    }
}

class CreateContext : FunctionEx<Processor.Context, TimeHolder> {
    override fun applyEx(ctx: Processor.Context) = TimeHolder()
}

class TimeHolder : Serializable {
    var value : Instant = Instant.now().minusSeconds(31)
        private set
    fun reset() {
        value = Instant.now()
    }
}