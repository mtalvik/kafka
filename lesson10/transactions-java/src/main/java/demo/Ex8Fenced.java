package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * Ex8 — fencing.
 *
 * Two producers use the SAME transactional.id = tx-ex8. This is the
 * zombie scenario: only one producer per transactional.id may be live.
 *
 *   producer1.initTransactions() + commit "from-1"   -> ok, epoch = e
 *   producer2.initTransactions()                      -> epoch bumped to e+1
 *   producer2 commit "from-2"                          -> ok
 *   producer1 tries another transaction                -> ProducerFencedException
 *
 * producer1 is now the zombie: it holds the stale epoch, the coordinator
 * rejects it, and it must be closed and recreated. This is exactly how a
 * stalled-then-resurrected instance is stopped from writing duplicates.
 */
public class Ex8Fenced {

    public static void main(String[] args) throws Exception {
        var props1 = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-ex8"));

        KafkaProducer<String, String> producer1 = new KafkaProducer<>(props1);
        producer1.initTransactions();
        producer1.beginTransaction();
        producer1.send(new ProducerRecord<>(Utils.TX_A, "from-1"));
        producer1.commitTransaction();
        System.out.println("producer1 committed");

        // second producer, same transactional.id -> bumps the epoch, fences producer1
        var props2 = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-ex8"));
        try (KafkaProducer<String, String> producer2 = new KafkaProducer<>(props2)) {
            producer2.initTransactions();
            producer2.beginTransaction();
            producer2.send(new ProducerRecord<>(Utils.TX_A, "from-2"));
            producer2.commitTransaction();
            System.out.println("producer2 committed (epoch bumped, producer1 now fenced)");
        }

        // producer1 is the zombie now
        try {
            producer1.beginTransaction();
            producer1.send(new ProducerRecord<>(Utils.TX_A, "from-1-again"));
            producer1.commitTransaction();
            System.out.println("producer1 committed again — UNEXPECTED, fencing failed");
        } catch (ProducerFencedException e) {
            System.out.println("ProducerFencedException as expected: producer1 is fenced, must be recreated");
        } finally {
            producer1.close();
        }
    }
}
