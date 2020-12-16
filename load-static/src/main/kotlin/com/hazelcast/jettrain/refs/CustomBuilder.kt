package com.hazelcast.jettrain.refs

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
        .batch("$filename-source", WithReader(filename, System.getProperty("data.path") ?: "/opt/hazelcast/data"))
        .fillBufferFn(WithFileLines)
        .destroyFn(CloseReader)
        .build()

object WithFileLines : BiConsumerEx<BufferedReader, SourceBuilder.SourceBuffer<String>> {
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

class WithReader(private val filename: String, private val root: String) : FunctionEx<Context, BufferedReader> {
    override fun applyEx(ctx: Context) =
        BufferedReader(
            FileReader(File("$root/infrastructure/data/current/$filename.txt"))
        )
}