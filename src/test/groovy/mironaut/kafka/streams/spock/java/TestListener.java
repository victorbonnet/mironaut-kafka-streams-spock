package mironaut.kafka.streams.spock.java;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
@KafkaListener(groupId = "test", clientId = "test-consumer")
public class TestListener {

    private final List<String> messages = new ArrayList<>();

    @Topic("output")
    public void eventOccurred(String body) {
        messages.add(body);
    }

    public List<String> getMessages() {
        return messages;
    }
}
