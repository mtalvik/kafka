package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Shared setup: SASL/PLAIN connection block, Streams config, run loop.
 */
final class Utils {

    private Utils() {}

    static final String EVENTS_TOPIC = "events";
    static final String COUNTS_TOPIC = "events-session-counts";
    static final String COUNTS_FINAL_TOPIC = "events-session-counts-final";

    /** Inactivity gap. Override while testing: -Dgap.minutes=1 */
    static Duration inactivityGap() {
        return Duration.ofMinutes(Long.getLong("gap.minutes", 5L));
    }

    static Properties streamsProps(String applicationId) {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }

        // application.id is also the consumer group id and the internal topic prefix.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Streams creates its changelog topic itself; without this it asks for the
        // broker's default.replication.factor=3 and dies on a single-node cluster.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // Emit every update instead of buffering.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        return props;
    }

    /** Print the topology, start, and block until SIGINT. */
    static void run(Topology topology, String applicationId) {
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, streamsProps(applicationId));
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(5));
            latch.countDown();
        }));

        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("streams thread died: " + e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        System.out.printf("app=%s gap=%s — waiting for records on '%s' (Ctrl-C to stop)%n",
                applicationId, inactivityGap(), EVENTS_TOPIC);
        streams.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Session windows are [start, end] in event time. */
    static String window(long startMs, long endMs) {
        return TIME.format(Instant.ofEpochMilli(startMs)) + ".." + TIME.format(Instant.ofEpochMilli(endMs));
    }
}
