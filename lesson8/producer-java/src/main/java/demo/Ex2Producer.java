package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;

public class Ex2Producer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        var props = Utils.producerConfig(p -> p.put(ProducerConfig.LINGER_MS_CONFIG, "500"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                final int idx = i;
                System.out.printf("%s send %d%n", LocalTime.now(), idx);
                producer.send(
                        new ProducerRecord<>(topic, null, "ex2 message " + idx),
                        (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.printf("%s callback %d partition=%d offset=%d%n",
                                        LocalTime.now(), idx,
                                        metadata.partition(), metadata.offset());
                            }
                        });
            }
            System.out.printf("%s loop done, waiting on close()%n", LocalTime.now());
        }
        System.out.printf("%s producer closed%n", LocalTime.now());
    }
}
