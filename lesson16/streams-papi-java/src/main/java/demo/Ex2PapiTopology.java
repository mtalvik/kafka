package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;

/**
 * Ex2 - a topology built by hand.
 *
 * No StreamsBuilder, no .stream(), no .mapValues(). Every node is named and
 * every parent is stated explicitly:
 *
 *     source ---> upper ---> sink
 *                       \--> logger
 *
 * Utils.start prints topology.describe() before starting, which is the first
 * thing to read: "-->" lists children, "<--" lists parents, and logger shows
 * "--> none" because it forwards nothing.
 */
public final class Ex2PapiTopology {

    public static void main(String[] args) {
        Properties props = Utils.streamProps("lesson16-ex2");

        Topology topology = new Topology();
        topology
                .addSource("source",
                        Serdes.String().deserializer(),
                        Serdes.String().deserializer(),
                        "papi-input")
                .addProcessor("upper", UpperProcessor::new, "source")
                .addProcessor("logger", LoggerProcessor::new, "upper")
                .addSink("sink", "papi-output",
                        Serdes.String().serializer(),
                        Serdes.String().serializer(),
                        "upper");

        Utils.start(topology, props);
    }

    /**
     * Uppercases the value and forwards to every child.
     *
     * LAB step 3.3: change the forward call to the addressed form
     *
     *     context.forward(record, "sink");
     *
     * and the logger stops receiving records while the sink keeps working.
     * The DSL has no equivalent - this is the reason to drop to this level.
     */
    static final class UpperProcessor implements Processor<String, String, String, String> {

        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            Record<String, String> out = record.withValue(record.value().toUpperCase());
            context.forward(out);
        }
    }

    /**
     * Terminal node: prints and forwards nothing. Shows up in describe() as
     * "--> none".
     */
    static final class LoggerProcessor implements Processor<String, String, String, String> {

        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            // recordMetadata() is empty for records produced by a punctuator
            // rather than read from a topic. Here it is always present.
            String origin = context.recordMetadata()
                    .map(m -> m.topic() + "-" + m.partition() + "@" + m.offset())
                    .orElse("punctuator");
            System.out.printf("logger: %s value=%s%n", origin, record.value());
        }
    }
}
