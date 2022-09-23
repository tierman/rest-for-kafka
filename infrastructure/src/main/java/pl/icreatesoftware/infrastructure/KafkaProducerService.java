package pl.icreatesoftware.infrastructure;

import com.google.gson.JsonObject;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.icreatesoftware.Employee;

import java.util.UUID;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<UUID, GenericRecord> kafkaTemplate;

    @Autowired
    KafkaProducerService(KafkaTemplate<UUID, GenericRecord> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Employee toSend, UUID key) {
        kafkaTemplate.send("topic1", key, toSend);
    }

    public void sendGeneric(String topic, String clientId, JsonObject data) {
        var key = UUID.randomUUID();
        var schemaUrl = "http://localhost:8888/";
        CachedSchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaUrl, 20);
        if (!topic.endsWith("-value")) {
            topic = topic + "-value";
        }
        try {
            var version = registryClient.getAllVersions(topic).get(0);
            Schema byVersion = registryClient.getByVersion(topic, version, false);
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            org.apache.avro.Schema schema = parser.parse(byVersion.getSchema());
            GenericRecord avroRecord = new GenericData.Record(schema);


            avroRecord.put("Name", "oooooo");
            avroRecord.put("Age", 111);
            //avroRecord.p

            kafkaTemplate.send(topic, key, avroRecord);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //kafkaTemplate.send("topic", key, );
        //kafkaTemplate.send("topic1", key, toSend);
    }

    public void registerSchema(String subject, boolean normalize, String schema) {
        var schemaUrl = "http://localhost:8888/";
        CachedSchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaUrl, 20);
        ParsedSchema parsedSchema = new AvroSchema(schema);
        try {
            registryClient.register(subject, parsedSchema, normalize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
