package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.impl.JetBootstrap
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.swisstrain.common.withCloseable

@Suppress("Unused")
class Stops {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            submit(stops)
        }
    }
}

@Suppress("Unused")
class Agencies {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            submit(agencies)
        }
    }
}

@Suppress("Unused")
class Routes {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            submit(routes)
        }
    }
}

@Suppress("Unused")
class Trips {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            submit(trips)
        }
    }
}

private fun submit(pipeline: Pipeline) {
    JetBootstrap.getInstance().withCloseable().use { jet ->
        jet.newJob(pipeline, jobConfig)
    }
} 