package demo;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ex6 — read-process-write, exactly once.
 *
 * Seeds tx-inbound with two records, then runs a transformer that:
 *   consume(tx-inbound) -> transform -> produce(tx-outbound)
 *   + commit the input offset, ALL in one transaction.
 *
 * The key call is sendOffsetsToTransaction(offsets, groupMetadata): the
 * input progress commits as part of the same transaction as the output
 * write, so a crash either commits both or neither — the outbound record
 * is written exactly once. enable.auto.commit MUST be false so the
 * consumer does not commit behind the transaction's back.
 *
 * Note the modern KIP-447 signature: ConsumerGroupMetadata, not String.
 */
public class Ex6ReadWrite {

    private static final String GROUP = "tx-ex6-group";

    public static void main(String[] args) throws Exception {
        // seed inbound
        try (KafkaProducer<String, String> seed = new KafkaProducer<>(Utils.producerConfig(p -> {}))) {
            seed.send(new ProducerRecord<>(Utils.TX_INBOUND, "k1", "first")).get();
            seed.send(new ProducerRecord<>(Utils.TX_INBOUND, "k2", "second")).get();
        }

        var producerProps = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-ex6-transformer"));

        var consumerProps = Utils.consumerConfig(GROUP, true, p ->
                p.put("enable.auto.commit", false));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaConsumer<String, String> verifier =
                     new KafkaConsumer<>(Utils.consumerConfig("tx-ex6-verify", true, p -> {}))) {

            producer.initTransactions();
            consumer.subscribe(List.of(Utils.TX_INBOUND));
            verifier.subscribe(List.of(Utils.TX_OUTBOUND));

            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                producer.beginTransaction();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, String> r : records) {
                        producer.send(new ProducerRecord<>(Utils.TX_OUTBOUND,
                                r.key(), r.value() + "-processed"));
                        offsets.put(new TopicPartition(r.topic(), r.partition()),
                                new OffsetAndMetadata(r.offset() + 1));
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                    System.out.println("transaction committed for " + records.count() + " record(s)");
                } catch (ProducerFencedException e) {
                    producer.close();
                    return;
                } catch (Exception e) {
                    producer.abortTransaction();
                }
            }

            long vDeadline = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < vDeadline) {
                ConsumerRecords<String, String> out = verifier.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : out) {
                    System.out.printf("tx-outbound: %s=%s%n", r.key(), r.value());
                }
            }
        }
    }
}
