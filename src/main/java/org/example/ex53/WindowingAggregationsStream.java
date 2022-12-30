package org.example.ex53;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.Topics;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.data.generators.CustomDateGenerator;
import org.example.base.stock.data.generators.DataGenerator;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.model.TransactionSummary;
import org.example.base.stock.serdes.StreamsSerdes;

public class WindowingAggregationsStream extends BaseStreamsApp {

  @Override
  public Topology createTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // serdes
    var stockTransactionSerde = StreamsSerdes.stockTransactionSerde();
    var transactionSummarySerde = StreamsSerdes.transactionSummarySerde();

    var inactivityGap = Duration.ofSeconds(20);

    /**
     * - source stream from stock-transaction
     * - groupBy TransactionSummary (who - customerId, sell/buy what - symbol)
     * - windowBy:
     *    (1) Session of 20 seconds inactivity gap
     *    (2)
     *    (3)
     * - aggregation: count by customerId & symbol
     */
    KGroupedStream<TransactionSummary, StockTransaction> transactionKGroupedStream = builder
            .stream(
                Topics.STOCK_TRANSACTIONS.topicName(),
                Consumed.with(Serdes.String(), stockTransactionSerde).withOffsetResetPolicy(
                    AutoOffsetReset.LATEST))
            .groupBy(
                (noKey, tx) -> TransactionSummary.from(tx),
                Grouped.<TransactionSummary, StockTransaction>as("by-tx-summary")
                    .withKeySerde(transactionSummarySerde)
                    .withValueSerde(stockTransactionSerde));
    KTable<Windowed<TransactionSummary>, Long> txCountByCustomerKTable;

    // Session windowing by gap of 20 seconds, stateStore retention within 15 min
//    txCountByCustomerKTable = transactionKGroupedStream
//        .windowedBy(
//        SessionWindows.ofInactivityGapWithNoGrace(inactivityGap)
//    )
//        .count(
//            Materialized.
//                <TransactionSummary, Long, SessionStore<Bytes, byte[]>>as(
//                "count-customer-stock-tx")
//                .withKeySerde(transactionSummarySerde)
//                .withValueSerde(Serdes.Long())
//                .withRetention(Duration.ofMinutes(15))
//        );

    // tumbling window
//    txCountByCustomerKTable = transactionKGroupedStream
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(20)))
//        .count(
//            Materialized.
//                <TransactionSummary, Long, WindowStore<Bytes, byte[]>>as(
//                "count-customer-stock-tx")
//                .withKeySerde(transactionSummarySerde)
//                .withValueSerde(Serdes.Long())
//                .withRetention(Duration.ofMinutes(15))
//        );

    // hopping window - size of 20 seconds, update every 5 seconds, overlap records
    txCountByCustomerKTable = transactionKGroupedStream
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
        );
    txCountByCustomerKTable.toStream()
        .print(
            Printed.<Windowed<TransactionSummary>, Long>toSysOut().withLabel("Customer Tx Counts"));

    /**
     * - convert KTable of stock tx counts into KStream
     * - change the key to industry of the count by stock symbol
     * Note: map -> repartitioning automatically
     */
    KStream<String, TransactionSummary> txCountByIndustry = txCountByCustomerKTable
        .toStream()
        .map((window, count) -> {
          TransactionSummary transactionSummary = window.key();
          String newKey = transactionSummary.getIndustry();
          transactionSummary.setSummaryCount(count);
          return KeyValue.pair(newKey, transactionSummary);
        });

    /**
     * - create KTable from financial-news using default key/value serde which is String
     * - Offset reset = EARLIEST => populate the whole topic data into KTable on startup
     */
    KTable<String, String> financialNewsKTable = builder.table(Topics.FINANCIAL_NEWS.topicName(),
        Consumed.with(AutoOffsetReset.EARLIEST));

    /**
     * - valueJoiner - combine the LEFT with the RIGHT -> return JOINED
     * - Joined.with - build Key, LEFT, and RIGHT serde using in join operation
     */
    ValueJoiner<TransactionSummary, String, String> valueJoiner = (txcnt, news) ->
        String.format("%d shares purchased [%s] related news [%s]",
            txcnt.getSummaryCount(),
            txcnt.getStockTicker(),
            news);
    KStream<String, String> txCountByIndustryWithNews = txCountByIndustry.leftJoin(
        financialNewsKTable,
        valueJoiner,
        Joined.with(
            Serdes.String(),
            transactionSummarySerde,
            Serdes.String()
        )
    );

    // for demonstration
    txCountByIndustryWithNews
        .print(Printed.<String, String>toSysOut().withLabel("Transactions & News"));

    return builder.build();
  }

  @Override
  public void mockData() {
    var dateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750));
    DataGenerator.setTimestampGenerator(dateGenerator::get);

    int numberOfIterations = 2;
    var numberOfTradedCompanies = 5;
    var numberOfCustomers = 3;
    var populateGlobalTables = false;

    MockDataProducer.produceStockTransactions(
        numberOfIterations, numberOfTradedCompanies, numberOfCustomers, populateGlobalTables);
  }
}
