package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx

sealed class IdExtractorFn(private val id: String) : FunctionEx<JsonObject?, String?> {
    override fun applyEx(entity: JsonObject?) =
        entity?.getString(id, null)
}

class GetAgencyId : IdExtractorFn("agency_id")
class GetRouteId : IdExtractorFn("route_id")

class MergeWithAgency : BiFunctionEx<JsonObject?, JsonObject?, JsonObject?> {
    override fun applyEx(entity: JsonObject?, ref: JsonObject?) =
        entity.apply {
            val agencyName = ref?.getString("agency_name", null)
            this?.set("agency_name", agencyName)
        }
}

class MergeWithRoute : BiFunctionEx<JsonObject?, JsonObject?, JsonObject?> {
    override fun applyEx(entity: JsonObject?, ref: JsonObject?) =
        entity.apply {
            val agencyName = ref?.getString("agency_name", null)
            val routeName = ref?.getString("route_name", null)
            val routeType = ref?.getString("route_type", null)
            this?.set("agency_name", agencyName)
                ?.set("route_name", routeName)
                ?.set("route_type", routeType)
        }
}