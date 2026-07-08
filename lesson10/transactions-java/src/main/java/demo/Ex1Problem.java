package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;

/**
 * Ex1 — the problem, idempotence OFF.
 *
 * A background producer sends keys 0,1,2,... to tx-a with
 * enable.idempotence=false. A consumer reads them back and reports any
 * key it did not expect: a GAP (missing) or a DUPLICATE (seen again).
 *
 * With a healthy network this runs clean. To SEE a duplicate you must
 * disrupt the producer→broker connection so a delivered record's ack is
 * lost and the library re-sends it. On the EC2 broker (see LAB.md):
 *     sudo ss -K dst 127.0.0.1 dport = 9092   # a few times while running
 * With idempotence off the broker cannot tell the resend from a new
 * record, so a duplicate key appears. Ex2 repeats this with idempotence
 * on and stays clean.
 */
public class Ex1Problem {

    public static void main(String[] args) throws Exception {
        boolean idempotence = false;   // Ex2 flips this to true
        runIdempotenceDemo(idempotence);
    }

    static void runIdempotenceDemo(boolean idempotence) throws Exception {
        Thread producer = new Thread(() -> produce(idempotence));
        producer.setDaemon(true);
        producer.start();

        var props = Utils.consumerConfig("tx-idem-verify", false, p -> {});
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Utils.TX_A));
            int expected = 0;
            long deadline = System.currentTimeMillis() + 60_000;   // run ~1 min
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    int key = Integer.parseInt(r.key());
                    if (key < expected) {
                        System.out.printf("DUPLICATE key=%d (expected >= %d) at offset %d%n",
                                key, expected, r.offset());
                    } else if (key > expected) {
                        System.out.printf("GAP: expected %d, got %d%n", expected, key);
                        expected = key + 1;
                    } else {
                        expected = key + 1;
                    }
                }
            }
        }
    }

    private static void produce(boolean idempotence) {
        var props = Utils.producerConfig(p -> {
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(idempotence));
            if (!idempotence) {
                p.put(ProducerConfig.ACKS_CONFIG, "1");   // idempotence off needs no acks=all
            }
        });
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int i = 0;
            while (!Thread.interrupted()) {
                producer.send(new ProducerRecord<>(Utils.TX_A, Integer.toString(i), "data-" + i));
                i++;
                if (i % 1000 == 0) producer.flush();
                Thread.sleep(2);
            }
        } catch (Exception ignored) {
        }
    }
}
