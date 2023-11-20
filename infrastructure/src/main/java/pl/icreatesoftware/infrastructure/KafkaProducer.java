package pl.icreatesoftware.infrastructure;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;
import pl.icreatesoftware.infrastructure.config.KafkaProducerConfig;
import pl.icreatesoftware.infrastructure.converter.JsonToAvroMessageConverter;

import java.util.*;

@Component
@Slf4j
public class KafkaProducer {

    private final SchemaRegistryClient schemaRegistryClient;
    private final JsonToAvroMessageConverter jsonToAvroMessageConverter;
    private final KafkaProducerConfig kafkaProducerConfig;

    private final ApplicationContext context;

    @Autowired
    KafkaProducer(SchemaRegistryClient schemaRegistryClient,
                  JsonToAvroMessageConverter jsonToAvroMessageConverter,
                  KafkaProducerConfig kafkaProducerConfig,
                  ApplicationContext context) {
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.schemaRegistryClient = schemaRegistryClient;
        this.jsonToAvroMessageConverter = jsonToAvroMessageConverter;
        this.context = context;
    }

    public void sendGeneric(String subjectName, String clientId, JsonObject json) {
        //TODO: create kafka template dynamic, and paste there specific config like clientId, address etc.
        var key = UUID.randomUUID();

        var parsedSchema = schemaRegistryClient.getNewestSchemaForSubject(subjectName);
        Schema schema = new Schema.Parser().parse(parsedSchema.toString());

        if (schema.isUnion()) {
            //todo: get last, most complete schema.
            schema = schema.getTypes().get(schema.getTypes().size() -1);
        }

        GenericRecord rootMessage = new GenericData.Record(schema);
        jsonToAvroMessageConverter.prepareMessageBasedOnJson(rootMessage, json);

        var kafkaTemplate = getKafkaTemplate(clientId);

        kafkaTemplate.send(subjectName, key, rootMessage);
    }

    private KafkaTemplate getKafkaTemplate(String clientId) {
        KafkaTemplate template = null;

        try {
            template = (KafkaTemplate) context.getBean("kafkaTemplate");
            template.getProducerFactory().updateConfigs(
                    kafkaProducerConfig.producerConfigWithCustomChanges(
                            null,
                            clientId,
                            null));
            template.getProducerFactory().reset();
        } catch (NoSuchBeanDefinitionException ex) {
            log.debug("Cannot find Kafka template in context: ", ex);
            template = new KafkaTemplate<>(
                    kafkaProducerConfig.buildProducerFactory(null, clientId, null));
        }

        return template;
    }
}
