package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Ex3 - stateful processor plus a wall-clock Punctuator.
 *
 * Sensors publish readings to sensor-readings (key = sensor id, value = a
 * number). The processor keeps a SLIDING WINDOW of the last WINDOW readings
 * per sensor in a state store. Every 10 seconds a punctuator sweeps the store,
 * compares the newest reading against the average of the window, and forwards
 * anything that moved by more than THRESHOLD_PCT to sensor-alerts.
 *
 * The window matters. Storing only the latest value and comparing it to a
 * fixed threshold would need no state at all - the record carries everything.
 * A rolling average cannot be computed from the current record, so the store
 * is doing real work. That is the whole point of a stateful processor.
 *
 * Two things to notice when it runs:
 *
 *   1. "punctuate" appears every 10 seconds before any record arrives at all.
 *      No DSL operator behaves like that.
 *   2. Two lines per tick, because sensor-readings has two partitions and the
 *      schedule lives inside the task, not the application.
 *
 * The shape comes from the stock-performance example in Bejeck's "Kafka
 * Streams in Action": a rolling sample of the last N trades, alerting in
 * batches rather than per event.
 */
public final class Ex3SensorPunctuate {

    private static final String STORE = "sensor-store";

    /** How many readings the rolling sample keeps per sensor. */
    private static final int WINDOW = 5;

    /** Percentage move against the window average that triggers an alert. */
    private static final double THRESHOLD_PCT = 20.0;

    public static void main(String[] args) {
        Properties props = Utils.streamProps("lesson16-ex3");

        StoreBuilder<KeyValueStore<String, String>> store =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE),
                        Serdes.String(),
                        Serdes.String());

        Topology topology = new Topology();
        topology
                .addSource("source",
                        Serdes.String().deserializer(),
                        Serdes.String().deserializer(),
                        "sensor-readings")
                .addProcessor("sensor", SensorProcessor::new, "source")
                .addStateStore(store, "sensor")   // store attached to the named processor
                .addSink("sink", "sensor-alerts",
                        Serdes.String().serializer(),
                        Serdes.String().serializer(),
                        "sensor");

        Utils.start(topology, props);
    }

    static final class SensorProcessor implements Processor<String, String, String, String> {

        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.store = context.getStateStore(STORE);

            // LAB step 4.3: swap WALL_CLOCK_TIME for STREAM_TIME and watch the
            // ticks stop the moment input goes quiet. Stream time is driven by
            // record timestamps, so with no records it never advances.
            //
            // schedule() returns a Cancellable. Nothing is cancelled here
            // because there is exactly one schedule per task, not one per key.
            context.schedule(
                    Duration.ofSeconds(10),
                    PunctuationType.WALL_CLOCK_TIME,
                    this::punctuate);
        }

        /**
         * Append the reading to this sensor's rolling sample, dropping the
         * oldest once the window is full.
         *
         * The sample is stored as a CSV string to keep the lab dependency-free
         * - no Avro, no JSON library, and the store contents stay readable
         * straight off the changelog topic with kafka-console-consumer.
         */
        @Override
        public void process(Record<String, String> record) {
            double reading;
            try {
                reading = Double.parseDouble(record.value().trim());
            } catch (NumberFormatException e) {
                System.out.printf("skip  sensor=%s value=%s (not a number)%n",
                        record.key(), record.value());
                return;
            }

            List<Double> window = parseWindow(store.get(record.key()));
            window.add(reading);
            while (window.size() > WINDOW) {
                window.remove(0);
            }

            store.put(record.key(), formatWindow(window));

            System.out.printf("read  sensor=%s value=%.1f window=%s%n",
                    record.key(), reading, window);
        }

        /**
         * Sweep every sensor, compare newest reading to the sample average.
         *
         * Note what this needs that a single record does not have: the
         * previous N readings. That is the state store earning its keep.
         */
        private void punctuate(long timestamp) {
            List<KeyValue<String, String>> alerts = new ArrayList<>();

            // Collect first, forward after: forwarding while iterating the
            // store you are reading is asking for trouble.
            try (KeyValueIterator<String, String> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, String> entry = it.next();
                    List<Double> window = parseWindow(entry.value);

                    // Not enough history yet - no verdict, not an alert.
                    if (window.size() < WINDOW) {
                        continue;
                    }

                    double latest = window.get(window.size() - 1);
                    double average = window.stream()
                            .mapToDouble(Double::doubleValue)
                            .average()
                            .orElse(0.0);

                    if (average == 0.0) {
                        continue;
                    }

                    double changePct = (latest - average) / average * 100.0;

                    if (Math.abs(changePct) > THRESHOLD_PCT) {
                        alerts.add(KeyValue.pair(entry.key, String.format(
                                "ALERT latest=%.1f avg=%.1f change=%+.1f%%",
                                latest, average, changePct)));
                    }
                }
            }

            System.out.printf("punctuate ts=%d scanned-store alerts=%d%n",
                    timestamp, alerts.size());

            for (KeyValue<String, String> alert : alerts) {
                context.forward(new Record<>(alert.key, alert.value, timestamp));
            }
        }

        private static List<Double> parseWindow(String csv) {
            if (csv == null || csv.isBlank()) {
                return new ArrayList<>();
            }
            return Arrays.stream(csv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new));
        }

        private static String formatWindow(List<Double> window) {
            return window.stream()
                    .map(d -> String.format("%.1f", d))
                    .collect(Collectors.joining(","));
        }
    }
}
