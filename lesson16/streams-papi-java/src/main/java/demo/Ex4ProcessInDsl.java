package demo;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.Properties;

/**
 * Ex4 - a processor inside a DSL chain.
 *
 * This is the normal production shape: the DSL does the plumbing, and a
 * processor node sits where the DSL runs out. Reads purchases (the lesson 15
 * topic), filters with a DSL operator, runs a FixedKeyProcessor via
 * processValues(), writes with to().
 *
 * processValues() and not process(): the processor only rewrites the value.
 * FixedKeyProcessor makes that a compile-time guarantee - a FixedKeyRecord
 * offers withValue() and nothing that could change the key - so Streams knows
 * no repartition is needed. Switching to process() would insert a repartition
 * topic into describe() even if the key were never actually touched.
 *
 * The interesting line is recordMetadata(): partition and offset of the record
 * being processed, information the DSL simply does not expose.
 */
public final class Ex4ProcessInDsl {

    public static void main(String[] args) {
        Properties props = Utils.streamProps("lesson16-ex4");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> purchases = builder.stream("purchases");

        purchases
                .filter((k, csv) -> Utils.amount(csv) > 5.0)
                .processValues(AuditProcessor::new)
                .to("papi-output");

        Utils.start(builder, props);
    }

    static final class AuditProcessor implements FixedKeyProcessor<String, String, String> {

        private FixedKeyProcessorContext<String, String> context;

        @Override
        public void init(FixedKeyProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            String origin = context.recordMetadata()
                    .map(m -> "partition=" + m.partition() + " offset=" + m.offset())
                    .orElse("no-metadata");

            String csv = record.value();
            System.out.printf("audit %s customer=%s dept=%s amount=%.2f%n",
                    origin, Utils.customerId(csv), Utils.department(csv), Utils.amount(csv));

            // withValue is the only mutation available here. There is no
            // withKey - that is the whole point of the fixed-key variant.
            context.forward(record.withValue(
                    Utils.department(csv) + ":" + Utils.amount(csv)));
        }
    }
}
