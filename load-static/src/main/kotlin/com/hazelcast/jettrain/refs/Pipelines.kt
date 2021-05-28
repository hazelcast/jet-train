package com.hazelcast.jettrain.refs

import com.hazelcast.jet.aggregate.AggregateOperations
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.pipeline.file.FileFormat
import com.hazelcast.jet.pipeline.file.FileSources
import com.hazelcast.jettrain.common.sampleEvery
import com.hazelcast.jettrain.common.toStringFn

internal val stops = pipeline(
    "stops",
    500,
    listOf("stop_id", "stop_name", "stop_lat", "stop_lon"),
    listOf("id", "stop_name", "stop_lat", "stop_long")
)
internal val agencies = pipeline(
    "agency",
    2,
    listOf("agency_id", "agency_name"),
    listOf("id", "agency_name")
)
internal val routes = pipeline(
    "routes",
    50,
    listOf("route_id", "agency_id", "route_short_name", "route_type", "route_color"),
    listOf("id", "agencyId", "route_name", "route_type", "route_color")
)
internal val trips = pipeline(
    "trips",
    500,
    listOf("trip_id", "route_id", "trip_headsign"),
    listOf("id", "routeId", "trip_headsign")
)
internal val stopTimes = Pipeline.create().apply {
    readFrom(file("stop_times", listOf("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence")))
        .groupingKey(ByKey)
        .aggregate(AggregateOperations.toList())
        .map(ToStopTime)
        .peek(sampleEvery(10_000), toStringFn)
        .map(ToEntryForStopTime)
        .writeTo(Sinks.map("stop_times"))
}

private fun pipeline(name: String, frequency: Int, columns: List<String>, fields: List<String>) =
    Pipeline.create().apply {
        readFrom(file(name, columns))
            .map(ToNameValuePair(fields))
            .map(ToJson)
            .peek(sampleEvery(frequency), toStringFn)
            .map(ToEntry)
            .writeTo(Sinks.map(name))
    }

private fun file(filename: String, columns: List<String>) =
    FileSources.files("${System.getProperty("data.path") ?: "/opt/hazelcast/data"}/infrastructure/data/current")
        .glob("$filename.txt")
        .format(FileFormat.csv(columns))
        .build()