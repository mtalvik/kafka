// producer-avro/src/main/java/com/example/produceravro/ProducerAvro.java
package com.example.produceravro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ProducerAvro {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        List<BasketEvent> items = Arrays.asList(
                BasketEvent.newBuilder()
                        .setOrderId("order-1").setProductId("sku-42").setQuantity(2).build(),
                BasketEvent.newBuilder()
                        .setOrderId("order-1").setProductId("sku-77").setQuantity(1).build()
        );

        try (KafkaProducer<String, BasketEvent> producer = new KafkaProducer<>(props)) {
            for (BasketEvent event : items) {
                ProducerRecord<String, BasketEvent> record =
                        new ProducerRecord<>("basket", event.getOrderId().toString(), event);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) exception.printStackTrace();
                    else System.out.printf("Отправлено в basket[%d]@%d%n",
                            metadata.partition(), metadata.offset());
                });
            }
            producer.flush();
        }
    }
}