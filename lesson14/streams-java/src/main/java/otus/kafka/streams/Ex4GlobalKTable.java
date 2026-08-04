package otus.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * GlobalKTable enrichment join (lecture section 7).
 *
 * The summary stream is joined against two GlobalKTables (companies, customers).
 * Each join picks its lookup key from the record VALUE via a KeyValueMapper, so
 * the stream keeps its own key: no selectKey, no repartition topics. That is the
 * whole reason to use a GlobalKTable here instead of a KTable.
 */
public final class Ex4GlobalKTable {

    public static void main(String[] args) {
        Serde<TransactionSummary> summarySerde = Json.serde(TransactionSummary.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TransactionSummary> summaries =
                builder.stream("transaction-summary", Consumed.with(Serdes.String(), summarySerde));

        GlobalKTable<String, String> companies =
                builder.globalTable("companies", Consumed.with(Serdes.String(), Serdes.String()));

        GlobalKTable<String, String> customers =
                builder.globalTable("customers", Consumed.with(Serdes.String(), Serdes.String()));

        summaries
                .join(companies,
                        (key, summary) -> summary.stockTicker(),
                        (summary, companyName) -> summary.withCompanyName(companyName))
                .join(customers,
                        (key, summary) -> summary.customerId(),
                        (summary, customerName) -> summary.withCustomerName(customerName))
                .selectKey((key, summary) -> summary.customerId())
                .to("enriched-summary", Produced.with(Serdes.String(), summarySerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), Config.load("lesson14-ex4"));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
