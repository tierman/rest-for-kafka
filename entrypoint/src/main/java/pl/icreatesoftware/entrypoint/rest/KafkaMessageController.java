package pl.icreatesoftware.entrypoint.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pl.icreatesoftware.infrastructure.KafkaProducerService;

import java.util.UUID;

@RestController
@RequestMapping("/api")
class KafkaMessageController {

    private final KafkaProducerService producerService;

    @Autowired
    KafkaMessageController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/index")
    public String index() {
        return "Greetings from Spring Boot!";
    }

    @PostMapping("/test-topic")
    public void testTopicPublication(@RequestBody String data) {
        producerService.send(data, UUID.randomUUID());
    }
}
