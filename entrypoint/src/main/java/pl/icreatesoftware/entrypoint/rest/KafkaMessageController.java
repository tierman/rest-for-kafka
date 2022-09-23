package pl.icreatesoftware.entrypoint.rest;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pl.icreatesoftware.entrypoint.rest.dto.Employee;
import pl.icreatesoftware.infrastructure.KafkaProducerService;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Slf4j
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
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Schema successfully registered."),
            @ApiResponse(responseCode = "500",
                    description = "Error when parsing schema.",
                    content = @Content(schema = @Schema(implementation = String.class)))
    })
    @PostMapping("/subject/{subject}/normalize/{normalize}/register")
    public ResponseEntity<?> registerSchemaOnSubject(
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

        int schemaId = 0;
        try {
            schemaId = producerService.registerSchema(subject, normalize, body);
        } catch (Exception ex) {
            //log.error(getExMessage(ex));
            return ResponseEntity.internalServerError().body(getExMessage(ex));
        }
        return ResponseEntity.ok(schemaId);
    }

    private static String getExMessage(Exception ex) {
        if (ex.getMessage() == null || ex.getMessage().isEmpty() || ex.getMessage().isBlank()) {
            return Arrays.stream(ex.getStackTrace()).limit(10)
                    .toList()
                    .toString();
        }
        return ex.getMessage();
    }
}
