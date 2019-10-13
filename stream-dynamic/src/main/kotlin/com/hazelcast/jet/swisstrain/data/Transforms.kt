package com.hazelcast.jet.swisstrain.data

import com.hazelcast.internal.json.Json
import com.hazelcast.jet.Traverser
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx
import java.io.Serializable
import java.util.*

class SplitPayload : FunctionEx<String, Traverser<String>> {
    override fun applyEx(payload: String): Traverser<String> {
        try {
            val json = Json.parse(payload).asObject()
            val entities = json.get("entity").toString()
            return TripTraverser(entities)
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }
}

class TripTraverser(entities: String) : Traverser<String>, Serializable {
    private val iterator = Json.parse(entities).asArray().iterator()
    override fun next() =
        if (iterator.hasNext()) iterator.next().toString()
        else null
}

class GetRouteId : FunctionEx<String, String> {
    override fun applyEx(entity: String) =
        Json.parse(entity).asObject()
            ?.get("trip_update")?.asObject()
            ?.get("trip")?.asObject()
            ?.get("route_id")?.asString()
}

class MergeWithRoute : BiFunctionEx<String, String, String> {
    override fun applyEx(entity: String, route: String?): String {
        if (route != null) {
            Json.parse(entity).asObject()?.apply {
                get("trip_update")?.asObject()
                    ?.get("trip")?.asObject()
                    ?.let {
                        val jsonRoute = Json.parse(route).asObject()
                        it.set("route", jsonRoute)
                        it.remove("route_id")
                        return toString()
                    }
            }
        }
        return entity
    }
}

class GetTripId : FunctionEx<String, String> {
    override fun applyEx(entity: String) =
        Json.parse(entity).asObject()
            ?.get("trip_update")?.asObject()
            ?.get("trip")?.asObject()
            ?.get("trip_id")?.asString()
}

class MergeWithTrip : BiFunctionEx<String, String, String> {
    override fun applyEx(entity: String, trip: String?): String {
        if (trip != null) {
            Json.parse(entity).asObject()?.apply {
                get("trip_update")?.asObject()
                    ?.get("trip")?.asObject()
                    ?.let {
                        val jsonTrip = Json.parse(trip).asObject()
                        it.merge(jsonTrip)
                        it.remove("trip_id")
                        return toString()
                    }
            }
        }
        return entity
    }
}

class GetAgencyId : FunctionEx<String, String> {
    override fun applyEx(entity: String) =
        Json.parse(entity).asObject()
            ?.get("trip_update")?.asObject()
            ?.get("trip")?.asObject()
            ?.get("route")?.asObject()
            ?.get("agency")?.asString()
}

class MergeWithAgency : BiFunctionEx<String, String, String> {
    override fun applyEx(entity: String, agency: String?): String {
        if (agency != null) {
            Json.parse(entity).asObject()?.apply {
                get("trip_update")?.asObject()
                    ?.get("trip")?.asObject()
                    ?.get("route")?.asObject()
                    ?.let {
                        val agencyJson = Json.parse(agency)
                        it.set("agency", agencyJson)
                        return toString()
                    }
            }
        }
        return entity
    }
}

class ToEntry : FunctionEx<String, Map.Entry<String, String>> {
    override fun applyEx(value: String): Map.Entry<String, String> {
        with(Json.parse(value).asObject()) {
            return AbstractMap.SimpleEntry(get("id").asString(), toString())
        }
    }
}