package com.hazelcast.jet.swisstrain

import java.time.Instant
import java.util.*
import com.hazelcast.core.Hazelcast
import com.hazelcast.jet.function.BiConsumerEx
import com.hazelcast.jet.function.ConsumerEx
import com.hazelcast.jet.pipeline.SinkBuilder
import com.hazelcast.jet.pipeline.SourceBuilder
import com.github.kittinunf.fuel.core.extensions.authentication
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result

private const val PATTERN = "{\"id\":\""

fun remoteService(url: String, token: String) = SourceBuilder
    .stream("http-source") {
        url.httpGet()
            .authentication()
            .bearer(token) to TimeHolder()
    }.fillBufferFn<String> { tuple, buffer ->
        if (Instant.now().isAfter(tuple.second.value.plusSeconds(30))) {
            val (_, _, result) = tuple.first.responseString()
            when (result) {
                is Result.Failure -> println(result.getException())
                is Result.Success -> {
                    result.get()
                        .splitToSequence(PATTERN)
                        .forEachIndexed { index, it ->
                            if (index != 0) buffer.add(PATTERN + it)
                        }
                }
            }
            tuple.second.reset()
        }
    }.build()

fun queue() = SinkBuilder
    .sinkBuilder<MutableList<String>>("update-sink") { LinkedList() }
    .receiveFn(BiConsumerEx<MutableList<String>, String> { list: MutableList<String>, item: String -> list.add(item) })
    .flushFn(ConsumerEx { Hazelcast.getHazelcastInstanceByName("hazelcastInstance").getQueue<String>("default").addAll(it) })
    .build()

private class TimeHolder(var value: Instant = Instant.now().minusSeconds(31)) {
    fun reset() {
        value = Instant.now()
    }
}
