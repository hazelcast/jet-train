package com.hazelcast.jettrain

import com.hazelcast.core.EntryEvent
import com.hazelcast.jet.Jet
import com.hazelcast.map.listener.EntryAddedListener
import com.hazelcast.map.listener.EntryUpdatedListener
import com.hazelcast.map.listener.MapListener
import org.springframework.http.HttpStatus
import org.springframework.messaging.simp.SimpMessageSendingOperations
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseStatus
import java.util.concurrent.ConcurrentLinkedQueue

@Controller
class UpdateController(val ops: SimpMessageSendingOperations) {

    private val listener = UpdateMapListener()

    init {
        Jet.newJetClient().hazelcastInstance
            .getMap<String, String>("update")
            .addEntryListener(listener, true)
    }

    @GetMapping("/data")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun readData() {
        while (true) {
            listener.poll()?.let {
                println(it)
                ops.convertAndSend("/topic/updates", it)
                Thread.sleep(100)
            }
        }
    }
}

class UpdateMapListener : MapListener, EntryAddedListener<String, String>,
    EntryUpdatedListener<String, String> {

    private val queue = ConcurrentLinkedQueue<String>()

    override fun entryAdded(event: EntryEvent<String, String>) {
        queue.add(event.value)
    }

    override fun entryUpdated(event: EntryEvent<String, String>?) {
        event?.value?.let { queue.add(it) }
    }

    fun poll(): String? = queue.poll()
}