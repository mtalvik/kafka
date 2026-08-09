package demo;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Ex1 - transactions in Kafka Streams.
 *
 *   gradle ex1 --no-daemon              at_least_once
 *   gradle ex1 --no-daemon -Peos=true   exactly_once_v2
 *
 * The topology is deliberately dull: read eos-input, tag the value, write
 * eos-output. Nothing about it changes between the two runs. The entire
 * difference is one config line in Utils.withOptionalEos - that is the point
 * of the exercise.
 *
 * What to look for after the -Peos=true run:
 *
 *   kafka-transactions.sh --bootstrap-server localhost:9092 \
 *     --command-config /tmp/admin.properties list
 *
 * A transactional id appears that exists in no config file anywhere in this
 * project. Streams derived it from application.id below.
 */
public final class Ex1Transaction {

    public static void main(String[] args) {
        Properties props = Utils.withOptionalEos(Utils.streamProps("lesson16-ex1"));

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("eos-input");

        input.peek((k, v) -> System.out.printf("in  key=%s value=%s%n", k, v))
             .mapValues(v -> v + " [processed]")
             .peek((k, v) -> System.out.printf("out key=%s value=%s%n", k, v))
             .to("eos-output");

        Utils.start(builder, props);
    }
}
