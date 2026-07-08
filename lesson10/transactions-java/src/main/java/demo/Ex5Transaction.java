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
 * Ex5 — atomic multi-topic write.
 *
 * A transactional producer writes one record to tx-a and one to tx-b
 * inside each transaction. Both land or neither does. Setting
 * transactional.id turns on transactions and implies idempotence.
 *
 * A read_committed consumer on tx-a prints only records from committed
 * transactions — which here is all of them, since every transaction
 * commits. (Ex7 shows what read_committed hides.)
 */
public class Ex5Transaction {

    public static void main(String[] args) throws Exception {
        var props = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-ex5"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(Utils.consumerConfig("tx-ex5-verify", true, p -> {}))) {

            consumer.subscribe(List.of(Utils.TX_A));

            producer.initTransactions();

            for (int i = 0; i < 4; i++) {
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(Utils.TX_A, "a-" + i));
                producer.send(new ProducerRecord<>(Utils.TX_B, "b-" + i));
                producer.commitTransaction();
                System.out.println("committed transaction " + i);
            }

            long deadline = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("read_committed sees tx-a: %s at offset %d%n",
                            r.value(), r.offset());
                }
            }
        }
    }
}
