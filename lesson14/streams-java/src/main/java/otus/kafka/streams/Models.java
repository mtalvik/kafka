package otus.kafka.streams;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Domain records for the lesson. All package-private, one source file. */

record StockTransaction(String ticker, String industry, long shares) {
}

record ShareVolume(String ticker, String industry, long shares) {

    static ShareVolume from(StockTransaction t) {
        return new ShareVolume(t.ticker(), t.industry(), t.shares());
    }

    ShareVolume plus(ShareVolume other) {
        return new ShareVolume(ticker, industry, shares + other.shares);
    }
}

record TransactionSummary(String customerId,
                          String stockTicker,
                          String industry,
                          int summaryCount,
                          String customerName,
                          String companyName) {

    TransactionSummary withCompanyName(String name) {
        return new TransactionSummary(customerId, stockTicker, industry, summaryCount, customerName, name);
    }

    TransactionSummary withCustomerName(String name) {
        return new TransactionSummary(customerId, stockTicker, industry, summaryCount, name, companyName);
    }
}

/**
 * Per-industry share totals keyed by ticker. Immutable: the adder and subtractor
 * return new instances, which is what the KGroupedTable aggregation needs.
 */
record IndustryTotals(Map<String, Long> byTicker) {

    static IndustryTotals empty() {
        return new IndustryTotals(new LinkedHashMap<>());
    }

    IndustryTotals add(String ticker, long shares) {
        Map<String, Long> copy = new LinkedHashMap<>(byTicker);
        copy.put(ticker, shares);
        return new IndustryTotals(copy);
    }

    IndustryTotals remove(String ticker) {
        Map<String, Long> copy = new LinkedHashMap<>(byTicker);
        copy.remove(ticker);
        return new IndustryTotals(copy);
    }

    String topN(int n) {
        return byTicker.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(n)
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(", "));
    }
}
