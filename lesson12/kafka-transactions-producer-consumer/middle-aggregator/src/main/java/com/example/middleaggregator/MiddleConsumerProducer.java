package com.example.middleaggregator;

import com.example.datacontracts.transacts.TransactProto.OrderAggregate;
import com.example.datacontracts.transacts.TransactProto.BasketItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class MiddleConsumerProducer {

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "middle-aggregator");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("isolation.level", "read_committed");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        producerProps.put("transactional.id", "middle-agg-tx-1");
        producerProps.put("enable.idempotence", "true");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("prices", "basket"));

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();

        ObjectMapper mapper = new ObjectMapper();

        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        Map<String, Object> avroConfig = new HashMap<>();
        avroConfig.put("schema.registry.url", "http://localhost:8081");
        avroConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        avroDeserializer.configure(avroConfig, false);

        Map<String, OrderAggregate.Builder> aggregates = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                producer.beginTransaction();

                try {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String orderId = record.key();
                        OrderAggregate.Builder builder = aggregates.computeIfAbsent(orderId,
                                id -> OrderAggregate.newBuilder().setOrderId(id));

                        if (record.topic().equals("prices")) {
                            JsonNode node = mapper.readTree(record.value());
                            builder.setPrice(node.get("price").asDouble());

                        } else if (record.topic().equals("basket")) {
                            GenericRecord avroRecord = (GenericRecord) avroDeserializer.deserialize(
                                    "basket", record.value());

                            String productId = avroRecord.get("productId").toString();
                            int quantity = (Integer) avroRecord.get("quantity");

                            builder.addItems(BasketItem.newBuilder()
                                    .setProductId(productId)
                                    .setQuantity(quantity)
                                    .build());
                        }

                        builder.setAggregatedAt(System.currentTimeMillis() / 1000);

                        OrderAggregate aggregate = builder.build();
                        producer.send(new ProducerRecord<>("transacts", orderId, aggregate.toByteArray()));
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (TopicPartition tp : records.partitions()) {
                        List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(tp);
                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(tp, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();

                } catch (Exception e) {
                    producer.abortTransaction();
                    e.printStackTrace();
                }
            }
        } finally {
            consumer.close();
            producer.close();
            avroDeserializer.close();
        }
    }
}