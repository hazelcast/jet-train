package com.hazelcast.jet.swisstrain

import com.hazelcast.core.EntryEvent
import com.hazelcast.internal.json.JsonObject
import com.hazelcast.jet.Jet
import com.hazelcast.map.listener.EntryAddedListener
import com.hazelcast.map.listener.EntryUpdatedListener
import com.hazelcast.map.listener.MapListener
import org.springframework.http.HttpStatus
import org.springframework.messaging.simp.SimpMessageSendingOperations
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseStatus
import java.util.*

@Controller
class UpdateController(val ops: SimpMessageSendingOperations) {

    private val listener = UpdateMapListener()

    init {
        Jet.newJetClient().hazelcastInstance
            .getMap<String, JsonObject>("update")
            .addEntryListener(listener, true)
    }

    @GetMapping("/data")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun readData() {
        while (true) {
            listener.poll()?.let {
                println(it)
                ops.convertAndSend("/topic/updates", it.toString())
                Thread.sleep(200)
            }
        }
    }
}

class UpdateMapListener : MapListener, EntryAddedListener<String, JsonObject>,
    EntryUpdatedListener<String, JsonObject> {

    private val queue = ArrayDeque<JsonObject>(150)

    override fun entryAdded(event: EntryEvent<String, JsonObject>) {
        queue.add(event.value)
    }

    override fun entryUpdated(event: EntryEvent<String, JsonObject>?) {
        event?.value?.let { queue.add(it) }
    }

    fun poll(): JsonObject? = queue.poll()
}