package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.time.Duration;

/**
 * Homework: count events with the same key within a 5-minute session.
 * A session for a key stays open while events keep arriving within the
 * 5-minute inactivity gap and closes once the gap elapses with no new event.
 */
public class Ex6SessionCount {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> events =
            builder.stream("events", Consumed.with(Serdes.String(), Serdes.String()));

        events.groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapAndGrace(
                Duration.ofMinutes(5), Duration.ofMinutes(1)))
            .count()
            .toStream()
            // Merging two sessions emits a tombstone for the window that was
            // absorbed: same key, null count. Drop those or the console fills
            // with count=null lines.
            .filter((windowedKey, count) -> count != null)
            .foreach((windowedKey, count) ->
                System.out.printf("key=%s window=[%s .. %s] count=%d%n",
                    windowedKey.key(),
                    windowedKey.window().startTime(),
                    windowedKey.window().endTime(),
                    count));

        Utils.start(builder, Utils.streamProps("lesson15-ex6"));
    }
}
