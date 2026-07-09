package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Producer with two transactions:
 *   1) send 5 messages to each of topic1, topic2  -> COMMIT
 *   2) send 2 messages to each of topic1, topic2  -> ABORT
 * A read_committed consumer must see only the 5+5 committed messages.
 */
public class TxProducer {

    public static void main(String[] args) {
        var props = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "hw3-producer"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();

            // Transaction 1 -> COMMIT
            producer.beginTransaction();
            for (int i = 1; i <= 5; i++) {
                producer.send(new ProducerRecord<>(Utils.TOPIC1, "key" + i, "committed-" + i));
                producer.send(new ProducerRecord<>(Utils.TOPIC2, "key" + i, "committed-" + i));
            }
            producer.commitTransaction();
            System.out.println("Transaction 1 COMMITTED: 5 messages to topic1 and 5 to topic2");

            // Transaction 2 -> ABORT
            producer.beginTransaction();
            for (int i = 1; i <= 2; i++) {
                producer.send(new ProducerRecord<>(Utils.TOPIC1, "key" + i, "aborted-" + i));
                producer.send(new ProducerRecord<>(Utils.TOPIC2, "key" + i, "aborted-" + i));
            }
            producer.abortTransaction();
            System.out.println("Transaction 2 ABORTED: 2 messages to each topic discarded");
        }
    }
}
