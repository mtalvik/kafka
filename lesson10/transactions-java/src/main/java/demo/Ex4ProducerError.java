package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;

/**
 * Ex4 — the boundary of idempotence: application-level resend.
 *
 * Idempotence dedups the library's OWN retries, within one producer
 * session. It does nothing when the application itself resends a record.
 *
 * Here the first producer sends keys 0..49 and closes. A second producer
 * (a new session, new PID) then replays from key 40 — simulating an
 * application that retried from an earlier point after a failure. The
 * verifier sees keys 40..49 twice. Idempotence was on the whole time and
 * did not help: to the broker the replays are new records with a new PID.
 *
 * This is the gap transactions close.
 */
public class Ex4ProducerError {

    private static final int FIRST_TO = 50;
    private static final int REPLAY_FROM = 40;

    public static void main(String[] args) throws Exception {
        // session 1: keys 0..49
        try (KafkaProducer<String, String> p1 = new KafkaProducer<>(Utils.producerConfig(p -> {}))) {
            for (int i = 0; i < FIRST_TO; i++) {
                p1.send(new ProducerRecord<>(Utils.TX_A, Integer.toString(i), "s1-" + i)).get();
            }
        }

        // session 2 (new PID): application "retries" from key 40 -> duplicates
        try (KafkaProducer<String, String> p2 = new KafkaProducer<>(Utils.producerConfig(p -> {}))) {
            for (int i = REPLAY_FROM; i < FIRST_TO; i++) {
                p2.send(new ProducerRecord<>(Utils.TX_A, Integer.toString(i), "s2-" + i)).get();
            }
        }

        // verify: count how many times each key appears
        var props = Utils.consumerConfig("tx-ex4-verify", false, p -> {});
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Utils.TX_A));
            int[] seen = new int[FIRST_TO];
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    int key = Integer.parseInt(r.key());
                    if (key < FIRST_TO) seen[key]++;
                }
            }
            for (int k = 0; k < FIRST_TO; k++) {
                if (seen[k] > 1) System.out.printf("key %d seen %d times (DUPLICATE)%n", k, seen[k]);
            }
            System.out.println("done — keys " + REPLAY_FROM + ".." + (FIRST_TO - 1)
                    + " should be duplicated; idempotence does not cross sessions");
        }
    }
}
