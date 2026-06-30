package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Ex4Producer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.baseProducerConfig())) {
            for (String key : new String[] {"order-42", "order-99"}) {
                for (int i = 0; i < 5; i++) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(topic, key, "ex4 " + key + " message " + i);
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("sent key=%s partition=%d offset=%d%n",
                            key, metadata.partition(), metadata.offset());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
