package pl.icreatesoftware.entrypoint.rest;

import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import pl.icreatesoftware.application.service.KafkaService;

@RestController
@RequestMapping("/api")
@Slf4j
class KafkaMessageController {

    private final KafkaService kafkaService;

    KafkaMessageController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @Operation(description = "<h2>Send message to the selected topic/subject</h2>",
            summary = "Send message to the selected topic/subject")
    @PostMapping("/topic/{topic}/client-id/{clientId}/send")
    public void sendGenericMessage(@RequestParam String topic,
                                   @RequestParam String clientId,
                                   @RequestBody String body) {

        kafkaService.sendGeneric(topic, clientId, body);
    }
}
