package com.hazelcast.jet.swisstrain

import com.hazelcast.jet.pipeline.SourceBuilder
import java.io.BufferedReader
import java.io.File
import java.io.FileReader

private class Anchor

fun file(filename: String) =
    SourceBuilder.batch("$filename-source") {
        reader(filename)
    }.fillBufferFn<String> { reader, buffer ->
        fill(reader, buffer)
    }.destroyFn { it?.close() }
        .build()

private fun reader(filename: String) = BufferedReader(
    FileReader(
        File(
            Anchor::class.java.classLoader.getResource("gtfsfp20192019-09-18/${filename}.txt").toURI()
        )
    )
)

private fun fill(reader: BufferedReader, buffer: SourceBuilder.SourceBuffer<String>) {
    repeat(128) {
        val line = reader.readLine()
        if (line != null) buffer.add(line)
    }
}