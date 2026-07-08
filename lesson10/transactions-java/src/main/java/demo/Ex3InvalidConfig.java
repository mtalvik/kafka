package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;

/**
 * Ex3 — invalid configuration.
 *
 * enable.idempotence=true is incompatible with
 * max.in.flight.requests.per.connection = 6 (the ceiling is 5).
 * KafkaProducer refuses to construct and throws ConfigException at
 * creation time — not later, at send time. Run this to see the exact
 * message the client gives you.
 */
public class Ex3InvalidConfig {

    public static void main(String[] args) {
        var props = Utils.producerConfig(p -> {
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 6);
        });

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("producer created — unexpected, config should have been rejected");
        } catch (ConfigException e) {
            System.out.println("ConfigException as expected: " + e.getMessage());
        }
    }
}
