package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;

// Manual partition assignment and offset control. No subscribe(), no group
// coordination: assign() one partition directly, then seekToBeginning() to
// replay it from offset 0 on every run, ignoring any committed offset.
// Shows that an offset is just a position in a partition.
public class Ex5Consumer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.baseConsumerConfig())) {
            TopicPartition p0 = new TopicPartition(topic, 0);
            consumer.assign(List.of(p0));
            consumer.seekToBeginning(List.of(p0));

            int emptyPolls = 0;
            while (emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("partition=%d offset=%d value=%s%n",
                            r.partition(), r.offset(), r.value());
                }
            }
        }
    }
}
