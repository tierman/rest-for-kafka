package pl.icreatesoftware.application.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import pl.icreatesoftware.infrastructure.KafkaProducer;

@Service
@Slf4j
public class KafkaService {

    private final KafkaProducer producer;

    public KafkaService(KafkaProducer producer) {
        this.producer = producer;
    }

    public void sendGeneric(String subjectName, String clientId, String body) {
        JsonObject json = JsonParser.parseString(body).getAsJsonObject();
        producer.sendGeneric(subjectName, clientId, json);
    }
}
