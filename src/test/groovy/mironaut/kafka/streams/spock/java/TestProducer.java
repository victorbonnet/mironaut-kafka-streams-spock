package mironaut.kafka.streams.spock.java;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient(id="product-client")
public interface TestProducer {

    @Topic("input")
    void send(@KafkaKey String key, String name);

    @Topic("table")
    void sendTable(@KafkaKey String key, String name);
}
