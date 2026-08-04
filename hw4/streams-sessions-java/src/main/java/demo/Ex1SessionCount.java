package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;

/**
 * Count events per key inside a session window.
 *
 * A session is a run of records for one key with no gap longer than the
 * inactivity gap between consecutive records. Unlike tumbling or hopping
 * windows, the boundaries are not fixed in advance — they are discovered from
 * the data, and two sessions merge into one as soon as a record arrives that
 * bridges them.
 *
 * This variant emits the running result on every record, so the console shows
 * the count moving. Ex2 shows the final-result-only variant.
 */
public class Ex1SessionCount {

    private static final String APP_ID = "hw4-session-count";

    public static void main(String[] args) {
        Duration gap = Utils.inactivityGap();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Windowed<String>, Long> counts = builder
                .stream(Utils.EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.printf("in   key=%s value=%s%n", key, value))
                // The console producer already sets the key, so groupByKey is enough.
                // groupBy() would re-key and force a repartition topic — one more
                // internal topic to create, and one more place the single-node
                // replication factor would bite.
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(gap))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("events-session-counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream();

        counts.foreach((windowedKey, count) -> {
            String w = Utils.window(windowedKey.window().start(), windowedKey.window().end());
            if (count == null) {
                // A null value is a tombstone: this session was absorbed into a
                // larger one, so its old window key no longer exists. Expect one
                // of these for every merge.
                System.out.printf("drop key=%s window=%s (session merged away)%n", windowedKey.key(), w);
            } else {
                System.out.printf("out  key=%s window=%s count=%d%n", windowedKey.key(), w, count);
            }
        });

        // Flatten the windowed key so the result topic can be read with a plain
        // console consumer.
        counts.filter((windowedKey, count) -> count != null)
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start() + "-" + windowedKey.window().end(),
                        count))
                .to(Utils.COUNTS_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        Utils.run(topology, APP_ID);
    }
}
