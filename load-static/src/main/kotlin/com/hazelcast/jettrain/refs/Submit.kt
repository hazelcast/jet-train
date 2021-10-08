package com.hazelcast.jettrain.refs

import com.hazelcast.core.Hazelcast
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jettrain.common.withCloseable

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

@Suppress("Unused")
class StopTimes {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            submit(stopTimes)
        }
    }
}

private fun submit(pipeline: Pipeline) {
    Hazelcast.bootstrappedInstance().withCloseable().use {
        it.jet.newJob(pipeline, jobConfig)
    }
} 