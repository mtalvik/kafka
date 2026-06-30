package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Ex1Producer {

    public static void main(String[] args) {
        String topic = Utils.topic();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.baseProducerConfig())) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, "ex1 message " + i);
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("sent partition=%d offset=%d%n",
                        metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
