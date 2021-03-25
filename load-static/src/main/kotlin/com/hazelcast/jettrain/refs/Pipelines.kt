package com.hazelcast.jettrain.refs

import com.hazelcast.jet.aggregate.AggregateOperations
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.pipeline.file.FileFormat
import com.hazelcast.jet.pipeline.file.FileSources

internal val stops = pipeline(
    "stops",
    listOf("stop_id", "stop_name", "stop_lat", "stop_lon"),
    listOf("id", "stop_name", "stop_lat", "stop_long")
)
internal val agencies = pipeline(
    "agency",
    listOf("agency_id", "agency_name"),
    listOf("id", "agency_name")
)
internal val routes = pipeline(
    "routes",
    listOf("route_id", "agency_id", "route_short_name", "route_type", "route_color"),
    listOf("id", "agencyId", "route_name", "route_type", "route_color")
)
internal val trips = pipeline(
    "trips",
    listOf("trip_id", "route_id", "trip_headsign"),
    listOf("id", "routeId", "trip_headsign")
)
internal val stopTimes = Pipeline.create().apply {
    readFrom(file("stop_times", listOf("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence")))
        .groupingKey { it[0] }
        .aggregate(AggregateOperations.toList())
        .map(ToStopTime)
        .peek()
        .map(ToEntryForStopTime)
        .writeTo(Sinks.map("stop_times"))
}

private fun pipeline(name: String, columns: List<String>, fields: List<String>) =
    Pipeline.create().apply {
        readFrom(file(name, columns))
            .map(ToNameValuePair(fields))
            .map(ToJson)
            .peek()
            .map(ToEntry)
            .writeTo(Sinks.map(name))
    }

private fun file(filename: String, columns: List<String>) =
    FileSources.files("${System.getProperty("data.path") ?: "/opt/hazelcast/data"}/infrastructure/data/current")
        .glob("$filename.txt")
        .format(FileFormat.csv(columns))
        .build()