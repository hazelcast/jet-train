package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.function.BiFunctionEx
import com.hazelcast.jet.function.FunctionEx

sealed class IdExtractorFn(private val id: String) : FunctionEx<JsonObject?, String?> {
    override fun applyEx(entity: JsonObject?) =
        entity?.getString(id, null)
}

class GetAgencyId : IdExtractorFn("agency")
class GetRouteId : IdExtractorFn("route")

sealed class MergerFn(
    private val member: String
) : BiFunctionEx<JsonObject?, JsonObject?, JsonObject?> {
    override fun applyEx(entity: JsonObject?, ref: JsonObject?) =
        if (ref != null) {
            entity?.set(member, ref)
        } else entity
}

class MergeWithAgency : MergerFn("agency")
class MergeWithRoute : MergerFn("route")