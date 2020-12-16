package com.hazelcast.jettrain.data

import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import com.hazelcast.function.BiConsumerEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.pipeline.SourceBuilder
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer
import java.io.Serializable
import java.time.Instant

fun remoteService(
    url: String = "http://api.511.org/transit/vehiclepositions",
    token: String
) = SourceBuilder
    .batch("http-source", CreateContext)
    .fillBufferFn(FillBuffer(url, token))
    .build()

class FillBuffer(private val url: String, private val token: String) : BiConsumerEx<TimeHolder, SourceBuffer<Pair<String, ByteArray>>> {
    override fun acceptEx(time: TimeHolder, buffer: SourceBuffer<Pair<String, ByteArray>>) {
        if (Instant.now().isAfter(time.value.plusSeconds(30))) {
            val (_, _, result) =
                Fuel.get(url)
                    .apply { parameters = listOf("agency" to "AC", "api_key" to token) }
                    .response()
            when (result) {
                is Result.Failure -> println(result.getException())
                is Result.Success -> buffer.add("AC" to result.get())
            }
            time.reset()
        }
    }
}

object CreateContext : FunctionEx<Processor.Context, TimeHolder> {
    override fun applyEx(ctx: Processor.Context) = TimeHolder()
}

class TimeHolder : Serializable {
    var value : Instant = Instant.now().minusSeconds(31)
        private set
    fun reset() {
        value = Instant.now()
    }
}