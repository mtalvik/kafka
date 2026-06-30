package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Ex5Producer {

    private static final int N = 100_000;
    private static final String PAYLOAD;

    static {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 200) {
            sb.append("payload ");
        }
        PAYLOAD = sb.toString();
    }

    public static void main(String[] args) {
        timeAcks("0");
        timeAcks("1");
        timeAcks("all");
    }

    private static void timeAcks(String acks) {
        String topic = Utils.topic();

        var props = Utils.producerConfig(p -> {
            p.put(ProducerConfig.ACKS_CONFIG, acks);
            // idempotence requires acks=all; disable explicitly for acks=0/1
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        });

        long start = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < N; i++) {
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), PAYLOAD));
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("acks=%s sent=%d elapsed=%d ms throughput=%.0f msg/s%n",
                acks, N, elapsed, N * 1000.0 / elapsed);
    }
}
