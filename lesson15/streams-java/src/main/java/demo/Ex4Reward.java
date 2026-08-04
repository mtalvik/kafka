package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Accumulate reward points per customer in a state store.
 * The input has no useful key, so records are repartitioned by customerId first,
 * guaranteeing every event for one customer lands on the same partition / Task.
 * The store is backed by a changelog topic: lesson15-ex4-rewards-store-changelog.
 */
public class Ex4Reward {

    private static final String STORE = "rewards-store";

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE),
                Serdes.String(),
                Serdes.Integer());
        builder.addStateStore(storeBuilder);

        KStream<String, String> purchases =
            builder.stream("purchases", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> byCustomer = purchases
            .selectKey((key, value) -> Utils.customerId(value))
            .repartition(Repartitioned.with(Serdes.String(), Serdes.String())
                .withName("reward-by-customer"));

        byCustomer.process(() -> new RewardProcessor(STORE), STORE)
            .print(Printed.<String, Integer>toSysOut().withLabel("BonusByCustomer"));

        Utils.start(builder, Utils.streamProps("lesson15-ex4"));
    }

    static final class RewardProcessor implements Processor<String, String, String, Integer> {

        private final String storeName;
        private KeyValueStore<String, Integer> store;
        private ProcessorContext<String, Integer> context;

        RewardProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<String, Integer> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, String> record) {
            String customerId = record.key();
            int points = (int) Math.floor(Utils.amount(record.value()) / 10.0);
            Integer previous = store.get(customerId);
            int total = (previous == null ? 0 : previous) + points;
            store.put(customerId, total);
            context.forward(record.withValue(total));
        }
    }
}
