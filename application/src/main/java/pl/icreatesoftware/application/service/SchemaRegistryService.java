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

    public int registerSchema(String subjectName, boolean normalize, String schema) {
        return schemaRegistryClient.registerSchema(subjectName, normalize, schema);
    }

    public String createJsonBasedOnLatestSchemaInSubject(String subjectName) {
        Schema schema = schemaRegistryClient.getNewestSchemaForSubject(subjectName);
        StringBuilder jsonBuilder = new StringBuilder();

        for (Object o : new RandomData(schema, 1)) {
            jsonBuilder.append(o);
        }

        return jsonBuilder.toString();
    }
}
