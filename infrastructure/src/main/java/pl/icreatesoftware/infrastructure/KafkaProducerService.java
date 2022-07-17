package pl.icreatesoftware.infrastructure;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<UUID, String> kafkaTemplate;

    @Autowired
    KafkaProducerService(KafkaTemplate<UUID, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String toSend, UUID key) {
        kafkaTemplate.send("topic1", key, toSend);
    }
}
