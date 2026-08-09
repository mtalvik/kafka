package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Shared Streams config for lesson 16. Loads the SASL/PLAIN connection block
 * from client.properties (path via -Dclient.properties.path, set by the gradle
 * task) and layers Streams settings on top.
 *
 * application.id doubles as the consumer group.id, the prefix for every
 * internal topic (changelog, repartition), AND - once transactions are on -
 * the base of the transactional.id. All three are covered by the prefixed
 * "lesson16-" ACLs granted to bob in lesson7/gitops.
 *
 * This is why application.id is a constant per exercise and never contains a
 * UUID: a randomised id produces a transactional.id no prefix ACL can cover,
 * and the run dies with TransactionalIdAuthorizationException.
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

    /**
     * Turns on exactly-once when the gradle task was run with -Peos=true.
     *
     * Note what this does NOT need: no transactional.id, no initTransactions,
     * no beginTransaction/commitTransaction. Streams owns all of it. Compare
     * with the hand-written producer loop in lesson 10.
     *
     * Setting the guarantee also drops the effective commit.interval.ms
     * default to 100 ms. The explicit 1000 above would override that, so it is
     * removed here to keep the lecture's number true in the lab.
     */
    static Properties withOptionalEos(Properties props) {
        if (Boolean.parseBoolean(System.getProperty("eos", "false"))) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            props.remove(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
            System.out.println("processing.guarantee = exactly_once_v2");
        } else {
            System.out.println("processing.guarantee = at_least_once (default)");
        }
        return props;
    }

    static void start(StreamsBuilder builder, Properties props) {
        start(builder.build(), props);
    }

    /**
     * Topology overload - the Processor API exercises build one directly
     * instead of going through StreamsBuilder.
     */
    static void start(Topology topology, Properties props) {
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        runUntilShutdown(streams);
    }

    static void runUntilShutdown(KafkaStreams streams) {
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
    // Same format as lesson 15 - ex4 reuses the purchases topic.
    static String field(String csv, int index) {
        String[] parts = csv.split(",", -1);
        return index < parts.length ? parts[index].trim() : "";
    }

    static String customerId(String csv) {
        return field(csv, 0);
    }

    static String department(String csv) {
        return field(csv, 2);
    }

    static double amount(String csv) {
        String a = field(csv, 3);
        return a.isEmpty() ? 0.0 : Double.parseDouble(a);
    }
}
