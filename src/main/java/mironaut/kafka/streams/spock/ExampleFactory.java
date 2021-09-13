package mironaut.kafka.streams.spock;

import groovy.util.logging.Slf4j;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.streams.kstream.Produced;

@Factory
@Slf4j
public class ExampleFactory {

    @Singleton
    @Named("example")
    KStream<String, String> exampleStream(ConfiguredStreamBuilder builder) {
        KStream<String, String> input = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));

        GlobalKTable<String, String> globalKTable = builder.globalTable("table", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> kStream = input.leftJoin(
                globalKTable,
                (k, v) -> v,
                (l, r) -> r
        );

        kStream
                .peek((k, v) -> {
                    System.out.println("builder = " + builder);
                })
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        return input;
    }
}
