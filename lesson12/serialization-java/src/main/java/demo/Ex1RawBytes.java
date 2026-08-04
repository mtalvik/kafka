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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * §2 — the broker is byte-transparent. Produce a JSON string with a
 * StringSerializer, then consume the value with a ByteArrayDeserializer:
 * the raw bytes the broker held. The broker parsed nothing; meaning is
 * added only when we decode on the read side.
 *
 * Touches the broker. Needs ser-demo (Step 1) and client.properties.
 */
public class Ex1RawBytes {

    public static void main(String[] args) throws Exception {
        OrderCreated order = Utils.sample();
        String json = new String(Utils.jsonSerialize(order));

        Properties p = Utils.connectionProps();
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (Producer<String, String> producer = new KafkaProducer<>(p)) {
            producer.send(new ProducerRecord<>(Utils.SER_DEMO, order.orderId, json)).get();
        }
        System.out.println("produced to " + Utils.SER_DEMO + ": " + json);

        Properties c = Utils.connectionProps();
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "ser-demo-ex1-" + System.currentTimeMillis());
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(c)) {
            consumer.subscribe(List.of(Utils.SER_DEMO));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> recs = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> r : recs) {
                    if (!order.orderId.equals(r.key()) || r.value() == null) {
                        continue;
                    }
                    byte[] value = r.value();
                    System.out.println("consumed raw value (" + value.length + " bytes): "
                            + Arrays.toString(value));
                    System.out.println("decoded as UTF-8: " + new String(value));
                    System.out.println("the broker stored and returned bytes; meaning was "
                            + "added by the deserializer");
                    return;
                }
            }
            System.out.println("no matching record within 10s");
        }
    }
}
