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
 * Ex7 — isolation.level and the Last Stable Offset.
 *
 * Two consumers read tx-a in parallel: one read_committed, one
 * read_uncommitted. A transactional producer and a plain producer
 * interleave sends:
 *
 *   begin transaction
 *   plain "0"           (outside tx, before anything open — visible to both)
 *   tx "1"              (inside tx — only read_uncommitted sees it now)
 *   plain "2"           (outside tx, but AFTER the tx opened — parked behind LSO)
 *   tx "3"              (inside tx)
 *   commit              -> read_committed now gets 1, 2, 3
 *   begin; tx "4"; abort -> "4" never visible to read_committed
 *   plain "END"
 *
 * Watch the two consumers' output. read_uncommitted prints everything as
 * it arrives; read_committed prints "0", then nothing until the commit,
 * then 1,2,3 together — including the plain "2", because it sat behind the
 * Last Stable Offset until the open transaction resolved. "4" (aborted)
 * never appears for read_committed.
 */
public class Ex7IsolationLevel {

    public static void main(String[] args) throws Exception {
        var txProps = Utils.producerConfig(p ->
                p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-ex7"));

        try (KafkaProducer<String, String> tx = new KafkaProducer<>(txProps);
             KafkaProducer<String, String> plain = new KafkaProducer<>(Utils.producerConfig(p -> {}));
             KafkaConsumer<String, String> rc =
                     new KafkaConsumer<>(Utils.consumerConfig("tx-ex7-rc", true, p -> {}));
             KafkaConsumer<String, String> ru =
                     new KafkaConsumer<>(Utils.consumerConfig("tx-ex7-ru", false, p -> {}))) {

            rc.subscribe(List.of(Utils.TX_A));
            ru.subscribe(List.of(Utils.TX_A));
            // let both join and seek to end-of-existing before we produce
            rc.poll(Duration.ofMillis(500));
            ru.poll(Duration.ofMillis(500));

            tx.initTransactions();

            tx.beginTransaction();
            plain.send(new ProducerRecord<>(Utils.TX_A, "0")).get();   // before-open, both see
            drain(rc, ru, 400);
            tx.send(new ProducerRecord<>(Utils.TX_A, "1"));            // in tx
            drain(rc, ru, 400);
            plain.send(new ProducerRecord<>(Utils.TX_A, "2")).get();   // outside, behind LSO
            drain(rc, ru, 400);
            tx.send(new ProducerRecord<>(Utils.TX_A, "3"));            // in tx
            drain(rc, ru, 400);

            System.out.println(">>> commitTransaction");
            tx.commitTransaction();                                    // rc now gets 1,2,3
            drain(rc, ru, 800);

            tx.beginTransaction();
            tx.send(new ProducerRecord<>(Utils.TX_A, "4"));            // will be aborted
            drain(rc, ru, 400);
            System.out.println(">>> abortTransaction");
            tx.abortTransaction();                                     // rc never sees 4

            plain.send(new ProducerRecord<>(Utils.TX_A, "END")).get();
            drain(rc, ru, 1500);
        }
    }

    private static void drain(KafkaConsumer<String, String> rc,
                              KafkaConsumer<String, String> ru, long ms) {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> r : rc.poll(Duration.ofMillis(100))) {
                System.out.println("  read_committed   : " + r.value());
            }
            for (ConsumerRecord<String, String> r : ru.poll(Duration.ofMillis(100))) {
                System.out.println("  read_uncommitted : " + r.value());
            }
        }
    }
}
