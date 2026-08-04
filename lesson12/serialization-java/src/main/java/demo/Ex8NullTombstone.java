package demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * §3 — null is a legal, meaningful value: a tombstone. The value serializer
 * returns null for null, the broker stores a record with a null value, and
 * the consumer reads value == null. Not an error, not an empty string — the
 * delete marker for log compaction (lesson 4).
 *
 * Touches the broker. Needs ser-demo (Step 1) and client.properties.
 */
public class Ex8NullTombstone {

    public static void main(String[] args) throws Exception {
        String key = "A-1001";

        Properties p = Utils.connectionProps();
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (Producer<String, String> producer = new KafkaProducer<>(p)) {
            producer.send(new ProducerRecord<>(Utils.SER_DEMO, key, null)).get();  // null value
        }
        System.out.println("produced key=" + key + " value=null (tombstone)");

        Properties c = Utils.connectionProps();
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "ser-demo-ex8-" + System.currentTimeMillis());
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.subscribe(List.of(Utils.SER_DEMO));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : recs) {
                    if (key.equals(r.key()) && r.value() == null) {
                        System.out.println("consumed key=" + r.key() + " value=" + r.value());
                        System.out.println("null survived the round-trip as a real record — on a "
                                + "compacted topic this deletes the key");
                        return;
                    }
                }
            }
            System.out.println("no tombstone seen within 10s");
        }
    }
}
