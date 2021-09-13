package mironaut.kafka.streams.spock.java;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class MicronautKafkaStreamsTest extends AbstractMironautKafkaStreamsTest {

    @Test
    void testItWorks() {
        testProducer.sendTable("t1", "Hello");
        testProducer.send("aaa", "t1");


        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> Assertions.assertEquals(1, testListener.getMessages().size()));
    }
}
