package org.example.ex62;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;
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

public class StockPerformanceProcessorStream extends BaseStreamsApp {
  static Logger LOG = LoggerFactory.getLogger(StockPerformanceProcessorStream.class);
  static final String stockStateStore = "stock-performance-state-store";
  static final String stockSource = "stock-source";
  static final String stockSink = "stock-sink";
  static final String stockProcessorName = "stock-processor";
  private double differentialThreshold = 0.02;



  @Override
  public Topology createTopology() {

    var topology = new Topology();

    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(
        stockStateStore);

    StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(
        storeSupplier,
        Serdes.String(),
        StreamsSerdes.stockPerformanceSerde()
    );

    /**
     * - add source from stock-transaction topic
     * - add stock processor
     * - add state store to processor
     * - add sink to write to stock-performance topic
     */
    topology.addSource(
        stockSource,
        Serdes.String().deserializer(),
        StreamsSerdes.stockTransactionSerde().deserializer(),
        Topics.STOCK_TRANSACTIONS.topicName())
        .addProcessor(
            stockProcessorName,
            () -> new StockPerformanceProcessor(
                stockStateStore,
                differentialThreshold
            ),
            stockSource
        )
        .addStateStore(
            storeBuilder,
            stockProcessorName
        ).connectProcessorAndStateStores(stockProcessorName, stockStateStore)
        .addSink(
            stockSink,
            Topics.STOCK_PERFORMANCE.topicName(),
            Serdes.String().serializer(),
            StreamsSerdes.stockPerformanceSerde().serializer(),
            stockProcessorName
        );

    topology.addProcessor("stock-printer",
        new KStreamPrint<>((k, v) -> LOG.info("[stock-transaction] K: {}, V: {}", k, v)),
        stockSource);

    topology.addProcessor("stock-performance-printer",
        new KStreamPrint<>((k, v) -> LOG.info("[stock-perf] K: {}, V: {}", k, v)),
        stockProcessorName);

    return topology;
  }

  @Override
  public void mockData() {
    MockDataProducer
        .produceStockTransactionsWithKeyFunction(50,50, 25, StockTransaction::getSymbol);
  }
}
