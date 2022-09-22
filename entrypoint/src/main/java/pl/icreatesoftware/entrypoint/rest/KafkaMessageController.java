package pl.icreatesoftware.entrypoint.rest;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pl.icreatesoftware.entrypoint.rest.dto.Employee;
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

    @PostMapping("/test-topic")
    public void testTopicPublication(@RequestBody Employee employee1) {
        pl.icreatesoftware.Employee employee = pl.icreatesoftware.Employee.newBuilder()
                .setName(employee1.getName())
                .setAge(employee1.getAge())
                .build();
        producerService.send(employee, UUID.randomUUID());
    }

    @PostMapping("/topic/{topic}/client-id/{clientId}/send")
    public void sendGenericMessage(@RequestParam String topic,
            @RequestParam String clientId,
            @RequestBody Gson data) {

        producerService.sendGeneric(topic, clientId, data);
    }

}
