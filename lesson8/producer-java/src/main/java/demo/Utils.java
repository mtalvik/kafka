package demo;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

final class Utils {

    private Utils() {}

    static Properties baseProducerConfig() {
        Properties props = new Properties();
        String path = System.getProperty("client.properties.path", "client.properties");
        try (FileInputStream in = new FileInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("failed to load " + path, e);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    static Properties producerConfig(Consumer<Properties> overrides) {
        Properties props = baseProducerConfig();
        overrides.accept(props);
        return props;
    }

    static String topic() {
        return baseProducerConfig().getProperty("topic", "producer-lab");
    }
}
