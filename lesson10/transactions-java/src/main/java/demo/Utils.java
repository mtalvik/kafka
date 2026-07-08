package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Shared config. Loads the SASL/PLAIN connection block from
 * client.properties (path via -Dclient.properties.path, set by the
 * gradle task) and layers serializers / deserializers on top.
 *
 * Topics and transactional.ids are pre-created by Terraform
 * (lesson7/gitops, tx- prefix). Nothing here creates or deletes topics.
 */
final class Utils {

    private Utils() {}

    // Topics provisioned in lesson7/gitops for this lab.
    static final String TX_A = "tx-a";
    static final String TX_B = "tx-b";
    static final String TX_INBOUND = "tx-inbound";
    static final String TX_OUTBOUND = "tx-outbound";

    /** SASL/PLAIN connection block shared by producers and consumers. */
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

    static Properties producerConfig(Consumer<Properties> overrides) {
        Properties props = connectionProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        overrides.accept(props);
        return props;
    }

    /**
     * Consumer config. group.id must start with "tx-" so alice's
     * prefixed Group ACL applies. auto.offset.reset=earliest so the
     * examples see messages produced just before the consumer starts.
     */
    static Properties consumerConfig(String groupId, boolean readCommitted, Consumer<Properties> overrides) {
        if (!groupId.startsWith("tx-")) {
            throw new IllegalArgumentException("group.id must start with tx- : " + groupId);
        }
        Properties props = connectionProps();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        overrides.accept(props);
        return props;
    }
}
