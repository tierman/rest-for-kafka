package pl.icreatesoftware.infrastructure.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaRegistryConfig {

    @Getter
    @Value("${rest-for-kafka.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${rest-for-kafka.schema.registry.subjects.value-suffix}")
    private boolean useValueSuffix;

    public String modifyTopicNameIfNeeded(String topic) {
        if (useValueSuffix && !topic.endsWith("-value")) {
            topic = topic + "-value";
        }
        return topic;
    }
}
