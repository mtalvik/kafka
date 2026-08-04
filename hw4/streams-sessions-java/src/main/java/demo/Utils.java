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
 * Shared setup: the SASL/PLAIN connection block, the Streams config that makes
 * the app survive on a single-node broker, and the run loop.
 */
final class Utils {

    private Utils() {}

    static final String EVENTS_TOPIC = "events";
    static final String COUNTS_TOPIC = "events-session-counts";
    static final String COUNTS_FINAL_TOPIC = "events-session-counts-final";

    /**
     * Inactivity gap. Five minutes is the assignment; override it while testing
     * so a session actually closes inside one sitting:  -Dgap.minutes=1
     */
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

        // application.id is also the consumer group id and the prefix of every
        // internal topic this app creates.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Single-node broker. Streams creates its changelog topic itself and, left
        // alone, asks for the broker's default.replication.factor. On a one-broker
        // cluster that request fails with INVALID_REPLICATION_FACTOR and the app
        // dies during startup. Same trap as __transaction_state and _schemas.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // Emit every update rather than buffering them: the point is to watch the
        // count move as records arrive. Note the config was renamed in 3.x —
        // cache.max.bytes.buffering is gone in 4.0.
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

    /** Session windows are [start, end] in event time — print them readably. */
    static String window(long startMs, long endMs) {
        return TIME.format(Instant.ofEpochMilli(startMs)) + ".." + TIME.format(Instant.ofEpochMilli(endMs));
    }
}
