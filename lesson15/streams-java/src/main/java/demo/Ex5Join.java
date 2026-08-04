package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;

/**
 * Issue a coupon when a customer appears in both the electronics and cafe streams
 * within a 20-minute window. Both inputs are keyed by customerId (console producer
 * with parse.key=true). Stream-stream joins are always windowed.
 */
public class Ex5Join {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> electronics =
            builder.stream("electronics-events", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> cafe =
            builder.stream("cafe-events", Consumed.with(Serdes.String(), Serdes.String()));

        electronics.join(
                cafe,
                (electronicsValue, cafeValue) -> "free-coffee-coupon",
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(20), Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .peek((key, value) -> System.out.println("coupon: customer=" + key + " -> " + value))
            .to("coupons", Produced.with(Serdes.String(), Serdes.String()));

        Utils.start(builder, Utils.streamProps("lesson15-ex5"));
    }
}
