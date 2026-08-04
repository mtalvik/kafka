package otus.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Aggregation and top-3 per industry (lecture section 4).
 *
 * stock-transactions -> total shares per company (KTable, reduce over a stream)
 *                    -> group that table by industry
 *                    -> aggregate with an adder AND a subtractor
 *                    -> emit top-3 tickers per industry to top-shares
 *
 * The subtractor is the point: because the per-company table updates in place,
 * the industry aggregate must remove a company's old contribution before adding
 * the new one. A stream aggregation would not have (or need) a subtractor.
 */
public final class Ex2Aggregation {

    public static void main(String[] args) {
        Serde<StockTransaction> txSerde = Json.serde(StockTransaction.class);
        Serde<ShareVolume> volumeSerde = Json.serde(ShareVolume.class);
        Serde<IndustryTotals> totalsSerde = Json.serde(IndustryTotals.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, StockTransaction> transactions =
                builder.stream("stock-transactions", Consumed.with(Serdes.String(), txSerde));

        KTable<String, ShareVolume> perCompany = transactions
                .mapValues(ShareVolume::from)
                .groupBy((key, volume) -> volume.ticker(),
                        Grouped.with(Serdes.String(), volumeSerde))
                .reduce(ShareVolume::plus,
                        Materialized.with(Serdes.String(), volumeSerde));

        KGroupedTable<String, ShareVolume> byIndustry = perCompany
                .groupBy((ticker, volume) -> KeyValue.pair(volume.industry(), volume),
                        Grouped.with(Serdes.String(), volumeSerde));

        KTable<String, IndustryTotals> totals = byIndustry.aggregate(
                IndustryTotals::empty,
                (industry, volume, agg) -> agg.add(volume.ticker(), volume.shares()),
                (industry, volume, agg) -> agg.remove(volume.ticker()),
                Materialized.with(Serdes.String(), totalsSerde));

        totals.mapValues(t -> t.topN(3))
                .toStream()
                .to("top-shares", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Config.load("lesson14-ex2"));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
