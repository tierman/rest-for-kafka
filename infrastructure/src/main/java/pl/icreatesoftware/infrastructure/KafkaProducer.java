package pl.icreatesoftware.infrastructure;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
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

    @Autowired
    KafkaProducer(SchemaRegistryClient schemaRegistryClient,
                  JsonToAvroMessageConverter jsonToAvroMessageConverter,
                  KafkaProducerConfig kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.schemaRegistryClient = schemaRegistryClient;
        this.jsonToAvroMessageConverter = jsonToAvroMessageConverter;
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

        var kafkaTemplate =  new KafkaTemplate<>(kafkaProducerConfig.producerFactory("localhost:29092", null, null));

        kafkaTemplate.send(subjectName, key, rootMessage);

        kafkaTemplate.destroy();
    }
}
