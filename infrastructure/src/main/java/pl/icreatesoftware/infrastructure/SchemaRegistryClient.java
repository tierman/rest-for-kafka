package pl.icreatesoftware.infrastructure;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import pl.icreatesoftware.infrastructure.config.SchemaRegistryConfig;

import java.util.Comparator;

@Component
public class SchemaRegistryClient {

    private final SchemaRegistryConfig schemaRegistryConfig;

    public SchemaRegistryClient(SchemaRegistryConfig schemaRegistryConfig) {
        this.schemaRegistryConfig = schemaRegistryConfig;
    }

    public ParsedSchema getNewestSchemaForSubject(String subjectName) {
        return getSchemaForSubject(subjectName, null);
    }

    public ParsedSchema getSchemaForSubject(String subjectName, Integer version) {
        var registryClient = getCachedSchemaRegistryClient();
        subjectName = schemaRegistryConfig.modifyTopicNameIfNeeded(subjectName);

        try {
            var latestVersion = version == null ?
                    registryClient.getAllVersions(subjectName).stream().max(Comparator.naturalOrder()).get()
                    : version;
            SchemaMetadata schemaMetadata = registryClient.getSchemaMetadata(subjectName, latestVersion);
            ParsedSchema parsedSchema = registryClient.getSchemaById(schemaMetadata.getId());
            return parsedSchema;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int registerSchema(String subjectName, String schema) {
        var registryClient = getCachedSchemaRegistryClient();
        subjectName = schemaRegistryConfig.modifyTopicNameIfNeeded(subjectName);

        ParsedSchema parsedSchema = new AvroSchema(schema);

        int schemaId;
        try {
            schemaId = registryClient.register(subjectName, parsedSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return schemaId;
    }

    private CachedSchemaRegistryClient getCachedSchemaRegistryClient() {
        var maxIdOfSchemaVersion = 20;
        return new CachedSchemaRegistryClient(
                schemaRegistryConfig.getSchemaRegistryUrl(),
                maxIdOfSchemaVersion);
    }
}
