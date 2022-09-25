package pl.icreatesoftware.entrypoint.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pl.icreatesoftware.application.service.SchemaRegistryService;

import java.util.Arrays;

@RestController
@RequestMapping("/api")
@Slf4j
class SchemaRegistryController {

    private final SchemaRegistryService schemaRegistryService;

    SchemaRegistryController(SchemaRegistryService schemaRegistryService) {
        this.schemaRegistryService = schemaRegistryService;
    }

    @Operation(description = "<h2>Register a new scheme for the selected topic/subject</h2>",
            summary = "Register a new scheme for the selected topic/subject")
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
            schemaId = schemaRegistryService.registerSchema(subject, normalize, body);
        } catch (Exception ex) {
            log.error(getExceptionMessage(ex));
            return ResponseEntity.internalServerError().body(getExceptionMessage(ex));
        }
        return ResponseEntity.ok(schemaId);
    }

    @Operation(description = "<h2>Get schema as json for the selected topic/subject</h2>",
            summary = "Get schema as json for the selected topic/subject")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Json successfully generated."),
            @ApiResponse(responseCode = "500",
                    description = "Error when parsing schema.",
                    content = @Content(schema = @Schema(implementation = String.class)))
    })
    @GetMapping("/subject/{subject}/json")
    public ResponseEntity<?> getRegisteredSchemaAsJsonForSubject(
            @Schema(type = "string",
                    description = "For envirnoment:<br> " +
                            "test - use topic/subject - 'test-topic-1'<br>" +
                            "prod - use topic/subject - 'topic-1'",
                    example = "test-topic-1")
            @PathVariable String subject) {

        String json;
        try {
            json = schemaRegistryService.createJsonBasedOnLatestSchemaInSubject(subject);
        } catch (Exception ex) {
            log.error(getExceptionMessage(ex));
            return ResponseEntity.internalServerError().body(getExceptionMessage(ex));
        }
        return ResponseEntity.ok(json);
    }

    private static String getExceptionMessage(Exception ex) {
        if (ex.getMessage() == null || ex.getMessage().isEmpty() || ex.getMessage().isBlank()) {
            return Arrays.stream(ex.getStackTrace()).limit(10)
                    .toList()
                    .toString();
        }
        return ex.getMessage();
    }
}
