package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.core.Processor.Context
import com.hazelcast.jet.function.BiConsumerEx
import com.hazelcast.jet.function.ConsumerEx
import com.hazelcast.jet.function.FunctionEx
import com.hazelcast.jet.pipeline.SourceBuilder
import java.io.BufferedReader
import java.io.File
import java.io.FileReader

fun file(filename: String) =
    SourceBuilder
        .batch("$filename-source", CreateReader(filename))
        .fillBufferFn(FillBuffer())
        .destroyFn(CloseReader)
        .build()

class FillBuffer : BiConsumerEx<BufferedReader, SourceBuilder.SourceBuffer<String>> {
    override fun acceptEx(reader: BufferedReader, buffer: SourceBuilder.SourceBuffer<String>) {
        repeat(128) {
            val line = reader.readLine()
            if (line != null) buffer.add(line)
            else buffer.close()
        }
    }
}

object CloseReader : ConsumerEx<BufferedReader> {
    override fun acceptEx(reader: BufferedReader) {
        reader.close()
    }
}

class CreateReader(private val filename: String) : FunctionEx<Context, BufferedReader> {
    override fun applyEx(ctx: Context) =
        BufferedReader(
            FileReader(File("/opt/hazelcast/data/gtfsfp20202020-02-19/$filename.txt"))
        )
}