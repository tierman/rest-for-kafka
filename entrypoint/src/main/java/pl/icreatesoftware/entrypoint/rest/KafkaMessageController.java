package pl.icreatesoftware.entrypoint.rest;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
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
                                   @RequestBody String body) {

        JsonObject jo = JsonParser.parseString(body).getAsJsonObject();
        producerService.sendGeneric(topic, clientId, jo);
    }

    @Operation(summary = "Register a new scheme for the selected topic/subject")
    @PostMapping("/subject/{subject}/normalize/{normalize}/register")
    public void registerSchemaOnSubject(
            @Schema(type = "string",
                    description = "For envirnoment:<br> " +
                    "test - use topic/subject - 'test-topic-1'<br>" +
                    "prod - use topic/subject - 'topic-1'",
                    example = "test-topic-1")
            @RequestParam String subject,
            @Schema(type = "boolean",
                    description = "The default is false. If you don't know what you should choose, leave false.",
                    example = "false")
            @RequestParam boolean normalize,
            @Schema(type = "string", example = "Paste schema from avro file with no changes")
            @RequestBody String body) {

        producerService.registerSchema(subject, normalize, body);
    }
}
