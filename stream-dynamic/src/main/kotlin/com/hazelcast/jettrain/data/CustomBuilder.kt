package com.hazelcast.jettrain.data

import com.github.kittinunf.fuel.httpGet
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

class WithEndpointData(
    private val token: String,
    private val url: String = "https://api.511.org/transit/vehiclepositions",
    private val agency: String = "AC",
    private val frequency: Long = 10
) : BiConsumerEx<TimeHolder, SourceBuffer<Pair<String, ByteArray>>> {
    override fun acceptEx(time: TimeHolder, buffer: SourceBuffer<Pair<String, ByteArray>>) {
        if (Instant.now().isAfter(time.value.plusSeconds(frequency))) {
            url.httpGet(listOf("agency" to agency, "api_key" to token))
                .response { _, _, result ->
                    when (result) {
                        is Result.Failure -> println(result.getException())
                        else -> buffer.add(agency to result.get())
                    }
                    time.reset()
                }.join()
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