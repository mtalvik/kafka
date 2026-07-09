package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumer with isolation.level=read_committed (set in Utils).
 * Reads topic1 and topic2; prints only committed messages.
 * The 2+2 aborted messages are never delivered.
 */
public class TxConsumer {

    public static void main(String[] args) {
        var props = Utils.consumerConfig("hw3-consumer", p -> {});

        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put(Utils.TOPIC1, 0);
        counts.put(Utils.TOPIC2, 0);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Utils.TOPIC1, Utils.TOPIC2));

            long deadline = System.currentTimeMillis() + 8_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("%s [p%d off%d] %s = %s%n",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value());
                    counts.merge(r.topic(), 1, Integer::sum);
                }
            }
        }

        System.out.println("----");
        counts.forEach((t, c) -> System.out.printf("%s: %d messages (read_committed)%n", t, c));
        System.out.println("Expected 5 per topic. Aborted messages are NOT shown.");
    }
}
