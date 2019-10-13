package com.hazelcast.jet.swisstrain

import com.hazelcast.jet.Jet
import org.springframework.http.HttpStatus
import org.springframework.messaging.simp.SimpMessageSendingOperations
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseStatus

@Controller
class UpdateController(val ops: SimpMessageSendingOperations) {

    @GetMapping("/data")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun readData() {
        Jet.newJetClient().hazelcastInstance
            .getMap<String, String>("update")
            .entries
            .take(3)
            .forEach {
                ops.convertAndSend("/topic/updates", it.value)
            }
    }
}