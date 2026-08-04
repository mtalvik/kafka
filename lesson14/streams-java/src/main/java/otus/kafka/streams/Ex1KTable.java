package otus.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;

/**
 * KTable and caching (lecture section 3).
 *
 * Reads stock-ticks as a table and logs every value it forwards downstream.
 * Run twice:
 *   ./gradlew ex1            -> default cache, intermediate updates deduplicated
 *   ./gradlew ex1 -Pcache=0  -> cache off, every update is forwarded
 */
public final class Ex1KTable {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> ticks =
                builder.table("stock-ticks", Consumed.with(Serdes.String(), Serdes.String()));

        ticks.toStream()
                .foreach((ticker, price) ->
                        System.out.println("forwarded: " + ticker + " = " + price));

        KafkaStreams streams = new KafkaStreams(builder.build(), Config.load("lesson14-ex1"));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
