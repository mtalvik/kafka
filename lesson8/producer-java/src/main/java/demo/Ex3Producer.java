package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Ex3Producer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        var props = Utils.producerConfig(p -> p.put(ProducerConfig.LINGER_MS_CONFIG, "200"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 200; i++) {
                final int idx = i;
                producer.send(
                        new ProducerRecord<>(topic, null, "ex3 message " + idx),
                        (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.printf("callback %d partition=%d offset=%d%n",
                                        idx, metadata.partition(), metadata.offset());
                            }
                        });

                if ((i + 1) % 50 == 0) {
                    System.out.printf("===== FLUSH at i=%d =====%n", i);
                    producer.flush();
                    System.out.printf("===== FLUSH done at i=%d =====%n", i);
                }
            }
        }
    }
}
