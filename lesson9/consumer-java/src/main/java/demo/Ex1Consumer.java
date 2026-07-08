package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

// Minimal consumer: subscribe, poll loop, auto-commit (default).
// Own group (lesson9-ex1) so it always reads producer-lab from the
// beginning. Drains the topic then exits after ~2.5s of idle polls.
public class Ex1Consumer {

    public static void main(String[] args) {
        String topic = Utils.topic();
        var config = Utils.consumerConfig(p ->
                p.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson9-ex1"));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));

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
            }
        }
    }
}
