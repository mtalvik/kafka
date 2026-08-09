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
import java.util.List;
import java.util.Properties;

/**
 * Ex3 - stateful processor plus a wall-clock Punctuator.
 *
 * Sensors publish readings to sensor-readings (key = sensor id, value = a
 * number). The processor writes the latest reading per sensor into a store on
 * every record. Every 10 seconds a punctuator sweeps the store and forwards
 * everything above THRESHOLD to sensor-alerts.
 *
 * Write per event, read on a timer. The DSL cannot express this shape.
 *
 * Two things to notice when it runs:
 *
 *   1. "punctuate" appears every 10 seconds before any record arrives at all.
 *      No DSL operator behaves like that.
 *   2. Two lines per tick, because sensor-readings has two partitions and the
 *      schedule lives inside the task, not the application.
 */
public final class Ex3SensorPunctuate {

    private static final String STORE = "sensor-store";
    private static final double THRESHOLD = 80.0;

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

        @Override
        public void process(Record<String, String> record) {
            store.put(record.key(), record.value());
            System.out.printf("read  sensor=%s value=%s%n", record.key(), record.value());
        }

        private void punctuate(long timestamp) {
            List<KeyValue<String, String>> breaches = new ArrayList<>();

            // Collect first, forward after: forwarding while iterating the
            // store you are reading is asking for trouble.
            try (KeyValueIterator<String, String> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, String> entry = it.next();
                    if (parse(entry.value) > THRESHOLD) {
                        breaches.add(entry);
                    }
                }
            }

            System.out.printf("punctuate ts=%d scanned-store breaches=%d%n",
                    timestamp, breaches.size());

            for (KeyValue<String, String> breach : breaches) {
                context.forward(new Record<>(
                        breach.key,
                        "ALERT " + breach.value + " > " + THRESHOLD,
                        timestamp));
            }
        }

        private static double parse(String value) {
            try {
                return Double.parseDouble(value.trim());
            } catch (NumberFormatException e) {
                return Double.NaN;   // NaN fails every comparison, so it never alerts
            }
        }
    }
}
