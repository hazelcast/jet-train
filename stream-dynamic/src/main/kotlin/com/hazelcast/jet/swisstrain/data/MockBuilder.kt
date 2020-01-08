package com.hazelcast.jet.swisstrain.data

import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.function.BiConsumerEx
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.SourceBuilder
import java.io.File
import java.io.Serializable
import java.time.Instant

fun mockService() = SourceBuilder
    .batch("mock-source", CreateMockContext())
    .fillBufferFn(MockBuffer())
    .build()

class MockBuffer : BiConsumerEx<Pair<TimeHolder, CountHolder>, SourceBuilder.SourceBuffer<String>> {
    override fun acceptEx(context: Pair<TimeHolder, CountHolder>, buffer: SourceBuilder.SourceBuffer<String>) {
        if (Instant.now().isAfter(context.first.value.plusSeconds(30))) {
            val url = this::class.java.classLoader.getResource("mock/data-0${context.second.value}.json")
            val content = File(url.toURI()).readText()
            buffer.add(content)
            context.first.reset()
            context.second.next()
        }
    }
}

class CreateMockContext : FunctionEx<Processor.Context, Pair<TimeHolder, CountHolder>> {
    override fun applyEx(ctx: Processor.Context) = TimeHolder() to CountHolder()
}

class CountHolder : Serializable {
    var value = 1
        private set

    fun next() {
        value++
        if (value == 9) value = 1
    }
}