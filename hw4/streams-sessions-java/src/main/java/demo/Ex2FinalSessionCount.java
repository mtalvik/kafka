package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;

/**
 * Same aggregation as Ex1, but only the final count per session is emitted.
 *
 * suppress(untilWindowCloses) holds every intermediate update in memory and
 * releases one record per session once stream time passes end + gap + grace.
 * Two consequences worth seeing:
 *
 *   1. No tombstones reach the output — merges are resolved inside the buffer.
 *   2. Nothing is emitted until a *later* record advances stream time. Stream
 *      time comes from record timestamps, not the wall clock, so a session for
 *      key A closes only when some record (any key) arrives with a timestamp
 *      past A's deadline. Stop producing and the last session never prints.
 */
public class Ex2FinalSessionCount {

    private static final String APP_ID = "hw4-session-count-final";

    public static void main(String[] args) {
        Duration gap = Utils.inactivityGap();

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(Utils.EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.printf("in   key=%s value=%s%n", key, value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(gap))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("events-session-counts-final-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((windowedKey, count) -> System.out.printf("final key=%s window=%s count=%d%n",
                        windowedKey.key(),
                        Utils.window(windowedKey.window().start(), windowedKey.window().end()),
                        count))
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start() + "-" + windowedKey.window().end(),
                        count))
                .to(Utils.COUNTS_FINAL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        Utils.run(topology, APP_ID);
    }
}
