package com.prosoft;

import com.prosoft.config.KafkaConfig;
import com.prosoft.domain.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Webinar-03: Kafka producer-service (отправка объектов класса Person)
 * Использования метода producer.send(producerRecord) с обработой результата отправки через Callback.
 */
public class KafkaProducerApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApp.class);
    private static final int MAX_MESSAGE = 10;

    public static void main(String[] args) {

        /** Использование try-with-resources для KafkaProducer */
        try (KafkaProducer<Long, Person> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig())) {

            for (int i = 0; i < MAX_MESSAGE; i++) {
                Person person = createPerson(i);

                /**
                 * Конструктор ProducerRecord(topic, key, value) принимает в качестве аргументов:
                 * - topic - номер топика
                 * - key - ключ id экземпляра Person (опция). С одним id всегда в одну партицию.
                 * - value - объект Person
                 *
                 * Варианты конструкторов:
                 * - ProducerRecord(topic, value)
                 * - ProducerRecord(topic, partition, timestamp, key, value)
                 * - ProducerRecord(topic, partition, key, value)
                 * - ProducerRecord(topic, partition, key, value, headers)
                 *
                 * Варианты ProducerRecord:
                 * 1) "Явное указание партиции": Жестко всегда в указанную партицию 0 в KafkaConfig.PARTITION
                 *    ProducerRecord<Long, Person> producerRecord = new ProducerRecord<>(KafkaConfig.TOPIC, KafkaConfig.PARTITION, person.getId(), person);
                 * 2) "Партицирование по ключу": указываем фиксированный ключ - номер партиции вычисляется как хэш от ключа. С одним ключом всегда сообщения уходят в одну партицию.
                 *    ProducerRecord<Long, Person> producerRecord = new ProducerRecord<>(KafkaConfig.TOPIC, 1L, person);
                 * 3) "round-robin" - если не указана партиция и ключ - применяется алгоритм round-robin: сообщения отправляются поочередно (циклически) в каждую партицию в топике
                 *    ProducerRecord<Long, Person> producerRecord = new ProducerRecord<>(KafkaConfig.TOPIC, person);
                 */
                ProducerRecord<Long, Person> producerRecord = new ProducerRecord<>(KafkaConfig.TOPIC, person.getId(), person);

                /** Анонимный внутренний класс (Callback), содержащий только один метод onCompletion() с записью через лямбду */
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error sending message: {}", e.getMessage(), e);
                    } else {
                        logger.info("Sent record: key={}, value={}, partition={}, offset={}",
                                person.getId(), person, recordMetadata.partition(), recordMetadata.offset());                        }
                });
                logger.info("Отправлено сообщение: key-{}, value-{}", i, person);
            }
            logger.info("Отправка завершена.");
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщений в Kafka", e);
        }
    }

    private static Person createPerson(int index) {
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss"));
        return new Person(index, "FirstName-" + currentTime, "LastName" + index, 20 + index);
    }

}
