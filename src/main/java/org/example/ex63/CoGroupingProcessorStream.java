package org.example.ex63;

import static org.example.ex63.CoGroupingProcessor.TUPLE_COGROUP_STORE_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.Topics;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.serdes.StreamsSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoGroupingProcessorStream extends BaseStreamsApp {

  static final Logger LOG = LoggerFactory.getLogger(CoGroupingProcessorStream.class);
  static final String stockTransactionSource = "stock-source";
  static final String clickEventSource = "click-event-source";
  static final String clickPerformanceSink = "click-performance-sink";
  static final String stockTxProcessor = "stock-transaction-processor";
  static final String clickEventProcessor = "click-event-processor";
  static final String coGroupingProcessor = "co-grouping-processor";


  @Override
  public Topology createTopology() {
    var topology = new Topology();

    var storeSupplier = Stores.persistentKeyValueStore(TUPLE_COGROUP_STORE_NAME);
    var storeBuilder = Stores.keyValueStoreBuilder(
        storeSupplier,
        Serdes.String(),
        StreamsSerdes.eventPerformanceTupleSerde()
    ).withLoggingEnabled(getChangelogConfig());

    topology
        .addSource(
            stockTransactionSource,
            Serdes.String().deserializer(),
            StreamsSerdes.stockTransactionSerde().deserializer(),
            Topics.STOCK_TRANSACTIONS.topicName())
        .addSource(
            clickEventSource,
            Serdes.String().deserializer(),
            StreamsSerdes.clickEventSerde().deserializer(),
            Topics.CLICK_EVENT.topicName())
        .addProcessor(
            stockTxProcessor,
            () -> new StockTransactionProcessor(),
            stockTransactionSource
        )
        .addProcessor(
            clickEventProcessor,
            () -> new ClickEventProcessor(),
            clickEventSource
        )
        .addProcessor(
            coGroupingProcessor,
            () -> new CoGroupingProcessor(),
            stockTxProcessor, clickEventProcessor
        )
        .addStateStore(storeBuilder, coGroupingProcessor)
        .addSink(
            clickPerformanceSink,
            Topics.CLICK_PERFORMANCE.topicName(),
            Serdes.String().serializer(), StreamsSerdes.eventPerformanceTupleSerde().serializer(),
            coGroupingProcessor
        );

    // for demonstration only
    topology.addProcessor("click-performance-printer",
        new KStreamPrint<>((k, v) -> LOG.info("[click-perf] K: {}, V: {}", k, v)),
        coGroupingProcessor);

    return topology;
  }

  public Map<String, String> getChangelogConfig() {
    var config = new HashMap<String, String>();
    config.put(TopicConfig.RETENTION_MS_CONFIG, "120000");
    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");
    return config;
  }

  @Override
  public Properties getConfig() {
    var streamConfig = super.getConfig();
    streamConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        WallclockTimestampExtractor.class);
    streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return streamConfig;
  }

  @Override
  public void mockData() {
    MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50, 100, 100,
        StockTransaction::getSymbol);
  }
}
