package mironaut.kafka.streams.spock.java;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.server.EmbeddedServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class AbstractMironautKafkaStreamsTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    protected static EmbeddedServer embeddedServer;
    protected static ApplicationContext context;
    protected static TestProducer testProducer;
    protected static TestListener testListener;

    private static void createTopics(List<String> topics) {
        Stream<NewTopic> newTopicStream = topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1));
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());

        AdminClient adminClient = AdminClient.create(properties);
        adminClient.createTopics(newTopicStream.collect(Collectors.toList()));
    }

    @Test
    void testItWorks() {
        kafka.start();

        createTopics(Arrays.asList("input","table","output"));

        Map<String, Object> props = new HashMap<>();
        props.put("kafka.bootstrap.servers", kafka.getBootstrapServers());

        embeddedServer = ApplicationContext.run(EmbeddedServer.class, props, "integration");

        context = embeddedServer.getApplicationContext();

        TestProducer testProducer = context.findBean(TestProducer.class).orElseThrow();
        TestListener testListener = context.findBean(TestListener.class).orElseThrow();

        testProducer.sendTable("t1", "Hello");
        testProducer.send("aaa", "t1");


        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> Assertions.assertEquals(1, testListener.getMessages().size()));
    }

    private List<String> topics() {
        return Arrays.asList("input","table","output");
    }

    @BeforeAll
    public static void start() {
        kafka.start();

        createTopics(Arrays.asList("input","table","output"));

        Map<String, Object> props = new HashMap<>();
        props.put("kafka.bootstrap.servers", kafka.getBootstrapServers());

        embeddedServer = ApplicationContext.run(EmbeddedServer.class, props, "integration");

        context = embeddedServer.getApplicationContext();

        testProducer = context.findBean(TestProducer.class).orElseThrow();
        testListener = context.findBean(TestListener.class).orElseThrow();
    }

    @AfterAll
    public static void clean() {
        kafka.stop();
        embeddedServer.close();
    }

}
