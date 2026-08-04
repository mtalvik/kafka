package demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Stateless processing of a purchase event (CSV: customerId,employeeId,department,amount,card).
 * Mask the card, then fan out to three sinks: masked storage, rewards, and a pattern view.
 */
public class Ex2PurchaseSimple {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());

        KStream<String, String> purchases =
            builder.stream("purchases", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> masked = purchases.mapValues(Ex2PurchaseSimple::maskCard);

        masked.print(Printed.<String, String>toSysOut().withLabel("masked"));
        masked.to("purchases-masked", produced);

        masked.mapValues(Ex2PurchaseSimple::rewardPoints).to("rewards", produced);
        masked.mapValues(Utils::department).to("patterns", produced);

        Utils.start(builder, Utils.streamProps("lesson15-ex2"));
    }

    private static String maskCard(String csv) {
        String card = Utils.cardNumber(csv);
        String last4 = card.length() >= 4 ? card.substring(card.length() - 4) : card;
        return Utils.customerId(csv) + "," + Utils.employeeId(csv) + ","
            + Utils.department(csv) + "," + Utils.field(csv, 3) + ",****" + last4;
    }

    private static String rewardPoints(String csv) {
        int points = (int) Math.floor(Utils.amount(csv) / 10.0);
        return Utils.customerId(csv) + "," + points;
    }
}
