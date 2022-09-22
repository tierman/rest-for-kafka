package pl.icreatesoftware.infrastructure;

import com.google.gson.Gson;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.icreatesoftware.Employee;

import java.io.IOException;
import java.util.Comparator;
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

    public void sendGeneric(String topic, String clientId, Gson data) {
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

            kafkaTemplate.send(topic, key, avroRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }
        //kafkaTemplate.send("topic", key, );
        //kafkaTemplate.send("topic1", key, toSend);
    }
}
