package org.example.ex64;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.Topics;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.model.StockPerformance;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.serdes.StreamsSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorApiAndKStreamApiStream extends BaseStreamsApp {

  static final Logger LOG = LoggerFactory.getLogger(ProcessorApiAndKStreamApiStream.class);

  private static final String stockPerformanceStore = "stock-performance-store";
  private static final double differentialThreshold = 0.02;

  @Override
  public Topology createTopology() {
    var builder = new StreamsBuilder();

    KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stockPerformanceStore,
        100);
    StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(
        storeSupplier,
        Serdes.String(),
        StreamsSerdes.stockPerformanceSerde());

    builder.addStateStore(storeBuilder);

    builder.stream(
            Topics.STOCK_TRANSACTIONS.topicName(),
            Consumed.with(Serdes.String(), StreamsSerdes.stockTransactionSerde())
        ).transform(
            () -> new StockPerformanceTransformer(stockPerformanceStore, differentialThreshold),
            stockPerformanceStore
        ).peek((k, v) -> LOG.info("[stock-performance], K: {}, V: {}", k, v))
        .to(
            Topics.STOCK_PERFORMANCE.topicName(),
            Produced.with(Serdes.String(), StreamsSerdes.stockPerformanceSerde())
        );

    return builder.build();
  }

  @Override
  public void mockData() {
    MockDataProducer.produceStockTransactionsWithKeyFunction(50, 50, 25, StockTransaction::getSymbol);
  }
}
