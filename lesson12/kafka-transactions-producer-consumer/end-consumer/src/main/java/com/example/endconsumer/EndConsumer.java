// end-consumer/src/main/java/com/example/endconsumer/EndConsumer.java
package com.example.endconsumer;

import com.example.datacontracts.transacts.TransactProto.OrderAggregate;
import com.example.datacontracts.transacts.TransactProto.BasketItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EndConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "end-consumer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("isolation.level", "read_committed");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("transacts"));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    OrderAggregate agg = OrderAggregate.parseFrom(record.value());

                    System.out.printf("OrderID: %s | Price: %.2f | Items: %d | AggregatedAt: %d%n",
                            agg.getOrderId(), agg.getPrice(), agg.getItemsCount(), agg.getAggregatedAt());

                    for (BasketItem item : agg.getItemsList()) {
                        System.out.printf("  - product: %s, qty: %d%n",
                                item.getProductId(), item.getQuantity());
                    }
                }
            }
        }
    }
}