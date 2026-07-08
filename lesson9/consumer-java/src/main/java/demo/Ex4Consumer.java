package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

// Hybrid commit. Own group (lesson9-ex4), auto-commit off.
// commitAsync() with a callback during the loop (fast, non-blocking),
// then a blocking commitSync() in finally to guarantee the last
// offsets land before close. The recommended production pattern.
public class Ex4Consumer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        var config = Utils.consumerConfig(p -> {
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson9-ex4");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        });

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(List.of(topic));

        try {
            int emptyPolls = 0;
            while (emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                            r.key(), r.value(), r.partition(), r.offset());
                }
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        System.out.println("async commit failed: " + exception.getMessage());
                    } else {
                        System.out.println("async committed: " + offsets);
                    }
                });
            }
        } finally {
            try {
                consumer.commitSync();
                System.out.println("final sync commit done");
            } finally {
                consumer.close();
            }
        }
    }
}
