package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Introductory topology: read src-topic, upper-case the value, write out-topic.
 * The peek node is a side branch that prints without altering the record.
 */
public class Ex1UpperCase {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
            builder.stream("src-topic", Consumed.with(Serdes.String(), Serdes.String()));

        source.filter((key, value) -> value != null)
            .mapValues(value -> value.toUpperCase())
            .peek((key, value) -> System.out.println("upper: " + key + " -> " + value))
            .to("out-topic", Produced.with(Serdes.String(), Serdes.String()));

        Utils.start(builder, Utils.streamProps("lesson15-ex1"));
    }
}
