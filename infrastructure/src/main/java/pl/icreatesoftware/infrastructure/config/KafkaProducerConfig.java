package pl.icreatesoftware.infrastructure.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
public class KafkaProducerConfig {
    @Value("${rest-for-kafka.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${rest-for-kafka.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${rest-for-kafka.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${rest-for-kafka.kafka.producer.client-id}")
    private String clientId;

    @Value("${rest-for-kafka.schema.registry.url}")
    private String schemaRegistryUrl;
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private Map<String, Object> producerConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);

        return config;
    }

    public ProducerFactory<UUID, Object> producerFactory(String bootstrapServers,
                                                         String clientId,
                                                         String schemaRegistryUrl) {
        var producerConfig = producerConfigs();

        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }

        if (clientId != null && !clientId.isBlank()) {
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }

        if (schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()) {
            producerConfig.put(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        }

        return new DefaultKafkaProducerFactory<>(producerConfig);
    }
}
