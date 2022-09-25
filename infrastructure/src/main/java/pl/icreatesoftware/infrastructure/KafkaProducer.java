package pl.icreatesoftware.infrastructure;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import pl.icreatesoftware.infrastructure.converter.JsonToAvroMessageConverter;

import java.util.*;

@Component
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<UUID, GenericRecord> kafkaTemplate;
    private final SchemaRegistryClient schemaRegistryClient;
    private final JsonToAvroMessageConverter jsonToAvroMessageConverter;

    @Autowired
    KafkaProducer(KafkaTemplate<UUID, GenericRecord> kafkaTemplate, SchemaRegistryClient schemaRegistryClient, JsonToAvroMessageConverter jsonToAvroMessageConverter) {
        this.kafkaTemplate = kafkaTemplate;
        this.schemaRegistryClient = schemaRegistryClient;
        this.jsonToAvroMessageConverter = jsonToAvroMessageConverter;
    }

    public void sendGeneric(String subjectName, String clientId, JsonObject json) {
        //TODO: create kafka template dynamic, and paste there specific config like clientId, address etc.
        var key = UUID.randomUUID();
        Schema schema = schemaRegistryClient.getNewestSchemaForSubject(subjectName);
        GenericRecord rootMessage = new GenericData.Record(schema);
        jsonToAvroMessageConverter.prepareMessageBasedOnJson(rootMessage, json);
        kafkaTemplate.send(subjectName, key, rootMessage);
    }
}
