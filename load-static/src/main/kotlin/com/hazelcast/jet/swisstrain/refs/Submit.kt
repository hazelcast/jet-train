package com.hazelcast.jet.swisstrain.refs

import com.hazelcast.jet.impl.JetBootstrap

@Suppress("Unused")
class Stops {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            execute(JetBootstrap.getInstance(), stops)
        }
    }
}

@Suppress("Unused")
class Agencies {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            execute(JetBootstrap.getInstance(), agencies)
        }
    }
}

@Suppress("Unused")
class Routes {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            execute(JetBootstrap.getInstance(), routes)
        }
    }
}

@Suppress("Unused")
class Trips {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            execute(JetBootstrap.getInstance(), trips)
        }
    }
}

@Suppress("Unused")
class StopTimes {
    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            execute(JetBootstrap.getInstance(), stopTimes)
        }
    }
}