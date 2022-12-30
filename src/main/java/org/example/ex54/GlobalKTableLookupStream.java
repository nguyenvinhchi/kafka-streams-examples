package org.example.ex54;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.WindowStore;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.Topics;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.data.generators.CustomDateGenerator;
import org.example.base.stock.data.generators.DataGenerator;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.model.TransactionSummary;
import org.example.base.stock.serdes.StreamsSerdes;

public class GlobalKTableLookupStream extends BaseStreamsApp {

  @Override
  public Topology createTopology() {
    var builder = new StreamsBuilder();

    // GlobalKTable for lookup small data
    GlobalKTable<String, String> customers = builder.globalTable(
        Topics.CLIENTS.topicName());
    GlobalKTable<String, String> publicCompanies = builder.globalTable(
        Topics.COMPANIES.topicName());

    // serdes
    var stockTransactionSerde = StreamsSerdes.stockTransactionSerde();
    var transactionSummarySerde = StreamsSerdes.transactionSummarySerde();

    var inactivityGap = Duration.ofSeconds(20);

    var transactionSummaryStream = builder
        .stream(
            Topics.STOCK_TRANSACTIONS.topicName(),
            Consumed.with(Serdes.String(), stockTransactionSerde).withOffsetResetPolicy(
                AutoOffsetReset.LATEST))
        .groupBy(
            (noKey, tx) -> TransactionSummary.from(tx),
            Grouped.<TransactionSummary, StockTransaction>as("by-tx-summary")
                .withKeySerde(transactionSummarySerde)
                .withValueSerde(stockTransactionSerde))
        .windowedBy(
            TimeWindows
                .ofSizeWithNoGrace(Duration.ofSeconds(20))
                .advanceBy(Duration.ofSeconds(5)))
        .count(
            Materialized.
                <TransactionSummary, Long, WindowStore<Bytes, byte[]>>as(
                "count-customer-stock-tx")
                .withKeySerde(transactionSummarySerde)
                .withValueSerde(Serdes.Long())
                .withRetention(Duration.ofMinutes(15))
        ).toStream()
        .map(
            (window, count) -> {
              TransactionSummary transactionSummary = window.key();
              String newKey = transactionSummary.getIndustry();
              if (count != null) {
                transactionSummary.setSummaryCount(count);
              }

              return KeyValue.pair(newKey, transactionSummary);
            }
        );

    KeyValueMapper<String, TransactionSummary, String> companyKeyValueMapper =
        (k, v) -> v.getStockTicker();
    ValueJoiner<TransactionSummary, String, TransactionSummary> companyValueJoiner =
        (k, companyName) -> k.withCompanyName(companyName);

    KeyValueMapper<String, TransactionSummary, String> customerKeyValueMapper =
        (k, v) -> v.getCustomerId();
    ValueJoiner<TransactionSummary, String, TransactionSummary> customerValueJoiner =
        (k, customerName) -> k.withCustomerName(customerName);

    KStream<String, TransactionSummary> enrichedTransactionSummaryStream = transactionSummaryStream
        .leftJoin(
            publicCompanies,
            companyKeyValueMapper,
            companyValueJoiner
        )
        .leftJoin(
            customers,
            customerKeyValueMapper,
            customerValueJoiner
        );

    enrichedTransactionSummaryStream
        .print(Printed.<String, TransactionSummary>toSysOut()
            .withLabel("Enriched Transaction Summary"));

    return builder.build();
  }

  @Override
  public void mockData() {
    var dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));
    DataGenerator.setTimestampGenerator(dateGenerator::get);

    int numberOfIterations = 2;
    var numberOfTradedCompanies = 5;
    var numberOfCustomers = 3;
    var populateGlobalTables = true;

    MockDataProducer.produceStockTransactions(
        numberOfIterations, numberOfTradedCompanies, numberOfCustomers, populateGlobalTables);
  }
}
