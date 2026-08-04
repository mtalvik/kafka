package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Shared Streams config. Loads the SASL/PLAIN connection block from
 * client.properties (path via -Dclient.properties.path, set by the gradle
 * task) and layers Streams settings on top.
 *
 * application.id doubles as the consumer group.id and the prefix for every
 * internal topic (changelog, repartition), so it must match the prefixed ACL
 * granted to the principal in lesson7/gitops.
 */
final class Utils {

    private Utils() {
    }

    private static Properties connectionProps() {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }
        return props;
    }

    static Properties streamProps(String applicationId) {
        Properties props = connectionProps();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Single-node broker: changelog and repartition topics inherit RF from
        // this config, whose default (-1) resolves to the broker default and
        // fails on one node.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "state/" + applicationId);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return props;
    }

    static void start(StreamsBuilder builder, Properties props) {
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Purchase record encoded as CSV: customerId,employeeId,department,amount,cardNumber
    static String field(String csv, int index) {
        String[] parts = csv.split(",", -1);
        return index < parts.length ? parts[index].trim() : "";
    }

    static String customerId(String csv) {
        return field(csv, 0);
    }

    static String employeeId(String csv) {
        return field(csv, 1);
    }

    static String department(String csv) {
        return field(csv, 2);
    }

    static double amount(String csv) {
        String a = field(csv, 3);
        return a.isEmpty() ? 0.0 : Double.parseDouble(a);
    }

    static String cardNumber(String csv) {
        return field(csv, 4);
    }
}
