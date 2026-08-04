// producer-json/src/main/java/com/example/producerjson/ProducerJson.java
package com.example.producerjson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class ProducerJson {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Введите orderId,price (например: order-1,199.90):");
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length != 2) {
                    System.out.println("формат: orderId,price");
                    continue;
                }

                try {
                    String orderId = parts[0].trim();
                    double price = Double.parseDouble(parts[1].trim());

                    PriceEvent event = new PriceEvent(orderId, price);
                    String json = mapper.writeValueAsString(event);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("prices", orderId, json);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.printf("Отправлено в prices[%d]@%d%n",
                                    metadata.partition(), metadata.offset());
                        }
                    });
                } catch (NumberFormatException e) {
                    System.out.println("некорректная цена: " + e.getMessage());
                }
            }
        }
    }
}