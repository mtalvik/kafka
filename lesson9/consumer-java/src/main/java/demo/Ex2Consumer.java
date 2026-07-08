package demo;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

// Consumer group scaling. Run this in 2-3 separate shells with the same
// group.id (from client.properties). producer-lab has 3 partitions, so the
// partitions split across the running instances. Ctrl-C one instance and
// watch the others log a new assignment (rebalance).
//
// Infinite loop; wakeup() from a shutdown hook breaks poll() cleanly.
public class Ex2Consumer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.baseConsumerConfig());
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("revoked: " + partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("assigned: " + partitions);
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                            r.key(), r.value(), r.partition(), r.offset());
                }
            }
        } catch (WakeupException e) {
            // expected on Ctrl-C
        } finally {
            consumer.close();
        }
    }
}
