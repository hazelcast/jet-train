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

fun remoteService(token: String) = SourceBuilder
    .stream("http-source", UsingTimeHolder)
    .fillBufferFn(WithEndpointData(token))
    .build()

class WithEndpointData(private val token: String) : BiConsumerEx<TimeHolder, SourceBuffer<Pair<String, ByteArray>>> {
    private val url = "https://api.511.org/transit/vehiclepositions"
    private val agency = "AC"
    override fun acceptEx(time: TimeHolder, buffer: SourceBuffer<Pair<String, ByteArray>>) {
        if (Instant.now().isAfter(time.value.plusSeconds(10))) {
            val (_, _, result) = Fuel.get(url)
                .apply { parameters = listOf("agency" to agency, "api_key" to token) }
                .response()
            if (result is Result.Failure) println(result.getException())
            else buffer.add(agency to result.get())
            time.reset()
        }
    }
}

object UsingTimeHolder : FunctionEx<Processor.Context, TimeHolder> {
    override fun applyEx(ctx: Processor.Context) = TimeHolder()
}

class TimeHolder : Serializable {
    internal var value: Instant = Instant.now()
        private set

    internal fun reset() {
        value = Instant.now()
    }
}