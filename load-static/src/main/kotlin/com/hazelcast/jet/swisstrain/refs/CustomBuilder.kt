package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.BiConsumerEx
import com.hazelcast.function.ConsumerEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.core.Processor.Context
import com.hazelcast.jet.pipeline.SourceBuilder
import java.io.BufferedReader
import java.io.File
import java.io.FileReader

fun file(filename: String) =
    SourceBuilder
        .batch("$filename-source", CreateReader(filename, System.getProperty("data.path") ?: "/opt/hazelcast/data"))
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

class CreateReader(private val filename: String, private val root: String) : FunctionEx<Context, BufferedReader> {
    override fun applyEx(ctx: Context) =
        BufferedReader(
            FileReader(File("$root/infrastructure/data/current/$filename.txt"))
        )
}