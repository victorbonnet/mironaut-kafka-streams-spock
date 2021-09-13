package mironaut.kafka.streams.spock

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.Property
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.testcontainers.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL

class TestEventConsumerSpec extends AbstractTestContainersSpec {

    def "Test message gets routed"() {
        given:
        TestConsumer consumer = embeddedServer.getApplicationContext().getBean(TestConsumer)
        TestKafkaClient client = embeddedServer.getApplicationContext().getBean(TestKafkaClient)

        when:
        // test
        client.sendTable("t1", "Hello")
        client.send("aaa", "t1")

        then: 'Make sure the message gets to the down stream topic eventually'
        conditions.eventually {
            consumer.messages.size() > 0
            consumer.messages[0] == "Hello"
            true
        }
    }

    @KafkaListener(
            groupId = "TestConsumer",
            properties = [
                    @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, value = 'org.apache.kafka.common.serialization.StringDeserializer'),
                    @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value = 'org.apache.kafka.common.serialization.StringDeserializer'),
            ],
            offsetReset = OffsetReset.EARLIEST,
            offsetStrategy = OffsetStrategy.AUTO
    )
    static class TestConsumer {
        List<String> messages = []

        @Topic('output')
        void receive(ConsumerRecord<String, String> consumerRecord) {
            messages.add(consumerRecord.value())
        }
    }

    @KafkaClient(acks = ALL)
    static interface TestKafkaClient {
        @Topic("input")
        void send(@KafkaKey String key, String name)

        @Topic("table")
        void sendTable(@KafkaKey String key, String name)
    }
}