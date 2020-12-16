package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.function.BiFunctionEx
import com.hazelcast.function.FunctionEx
import com.hazelcast.internal.json.Json

sealed class IdExtractorFn(private val id: String) : FunctionEx<String?, String?> {
    override fun applyEx(string: String?) = Json.parse(string)
        ?.asObject()
        ?.getString(id, null)
}

object GetAgencyId : IdExtractorFn("agency_id")
object GetRouteId : IdExtractorFn("route_id")

object MergeWithAgency : BiFunctionEx<String?, String?, String?> {
    override fun applyEx(string: String?, stringRef: String?): String? {
        val entity = Json.parse(string)?.asObject()
        val ref = Json.parse(stringRef)?.asObject()
        return entity?.apply {
            val agencyName = ref?.getString("agency_name", null)
            set("agency_name", agencyName)
        }?.toString()
    }
}

object MergeWithRoute : BiFunctionEx<String?, String?, String?> {
    override fun applyEx(string: String?, stringRef: String?): String? {
        val entity = Json.parse(string)?.asObject()
        val ref = Json.parse(stringRef)?.asObject()
        return entity?.apply {
            val agencyName = ref?.getString("agency_name", null)
            val routeName = ref?.getString("route_name", null)
            val routeType = ref?.getString("route_type", null)
            set("agency_name", agencyName)
                ?.set("route_name", routeName)
                ?.set("route_type", routeType)
        }?.toString()
    }
}