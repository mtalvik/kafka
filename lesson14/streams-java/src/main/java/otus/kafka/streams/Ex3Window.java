package otus.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

/**
 * Windowed counts with suppression (lecture section 5).
 *
 * Counts transactions per ticker in 10-second TUMBLING windows (advance = size),
 * with a 5-second grace period, and suppresses output until each window closes,
 * so exactly one final count per window is emitted instead of a stream of partials.
 */
public final class Ex3Window {

    public static void main(String[] args) {
        Serde<StockTransaction> txSerde = Json.serde(StockTransaction.class);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("stock-transactions", Consumed.with(Serdes.String(), txSerde))
                .selectKey((key, tx) -> tx.ticker())
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key()
                                + "@" + windowedKey.window().start()
                                + "/" + windowedKey.window().end(),
                        count))
                .to("windowed-counts", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Config.load("lesson14-ex3"));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
