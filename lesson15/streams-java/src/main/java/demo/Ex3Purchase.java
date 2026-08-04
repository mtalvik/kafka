package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/**
 * filter + selectKey to keep expensive purchases re-keyed by customer;
 * split() to route cafe and electronics sales to separate topics;
 * foreach as a terminal external-write node.
 */
public class Ex3Purchase {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        KStream<String, String> purchases =
            builder.stream("purchases", Consumed.with(Serdes.String(), Serdes.String()));

        purchases.filter((key, value) -> Utils.amount(value) > 100.0)
            .selectKey((key, value) -> Utils.customerId(value))
            .to("expensive-purchases", produced);

        // split() replaces the removed branch(Predicate...) array API (KIP-418, removed in 4.0)
        purchases.split(Named.as("dept-"))
            .branch((key, value) -> "cafe".equals(Utils.department(value)),
                Branched.withConsumer(ks -> ks.to("cafe-sales", produced)))
            .branch((key, value) -> "electronics".equals(Utils.department(value)),
                Branched.withConsumer(ks -> ks.to("electronics-sales", produced)))
            .noDefaultBranch();

        purchases.foreach((key, value) ->
            System.out.println("persist employee=" + Utils.employeeId(value) + " : " + value));

        Utils.start(builder, Utils.streamProps("lesson15-ex3"));
    }
}
