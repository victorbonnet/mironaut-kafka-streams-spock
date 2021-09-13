package mironaut.kafka.streams.spock

import groovy.util.logging.Slf4j
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@Slf4j
abstract class AbstractTestContainersSpec extends Specification {

    PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    static KafkaContainer kafkaContainer = new KafkaContainer("5.4.2")

    def setupSpec() {
        kafkaContainer.start()

        createTopics(getTopics())

        List<Object> config = ["kafka.bootstrap.servers", "${kafkaContainer.getBootstrapServers()}"]
        config.addAll(getConfiguration())

        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        (config as Object[])
                )
        )

        context = embeddedServer.getApplicationContext()
    }

    //Override or extend to create different properties for specs
    protected List<Object> getConfiguration() {
        return [
                'some.property','a',
                'some.property2','b'
        ]
    }

    //Override or extend to create topics on startup
    protected List<String> getTopics() {
        return ['input','table','output']
    }

    private static void createTopics(List<String> topics) {
        def newTopics = topics.collect { topic -> new NewTopic(topic, 1, (short) 1) }
        def admin = AdminClient.create(["bootstrap.servers": kafkaContainer.getBootstrapServers()])
        admin.createTopics(newTopics)
    }

    def cleanupSpec() {
        try {
            kafkaContainer.stop()
            embeddedServer.stop()
            log.warn("Stopped containers!")
        } catch (Exception ignore) {
            log.error("Could not stop containers")
        }
        if (embeddedServer != null) {
            embeddedServer.close()
        }
    }
}