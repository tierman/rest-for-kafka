package pl.icreatesoftware.application.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.util.RandomData;
import org.springframework.stereotype.Service;
import pl.icreatesoftware.infrastructure.SchemaRegistryClient;

@Service
@Slf4j
public class SchemaRegistryService {

    private final SchemaRegistryClient schemaRegistryClient;

    public SchemaRegistryService(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public int registerSchema(String subjectName, String schema) {
        return schemaRegistryClient.registerSchema(subjectName, schema);
    }

    public String createJsonBasedOnLatestSchemaInSubject(String subjectName) {
        var parsedSchema = schemaRegistryClient.getNewestSchemaForSubject(subjectName);
        var schema = new Schema.Parser().parse(parsedSchema.toString());
        StringBuilder jsonBuilder = new StringBuilder();

        for (Object o : new RandomData(schema, 10, true)) {
            jsonBuilder.append(o);
        }

        return jsonBuilder.toString();
    }
}
