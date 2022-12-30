package org.example.ex52;

import java.text.NumberFormat;
import java.util.Iterator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.Topics;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.model.ShareVolume;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.serdes.StreamsSerdes;
import org.example.base.stock.util.FixedSizePriorityQueue;
import org.example.base.stock.util.ShareVolumeComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockTopFiveCompanyStream extends BaseStreamsApp {

  private static final Logger LOG = LoggerFactory.getLogger(StockTopFiveCompanyStream.class);

  @Override
  public Topology createTopology() {
    var builder = new StreamsBuilder();
    Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.stockTransactionSerde();
    Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.shareVolumeSerde();

    /**
     * create KTable on rolling update total of share volume:
     * - source stream from stock-transactions
     * - transform StockTransaction -> ShareVolume to strip metadata
     * - groupBy share symbol as key (keyValueMapper func, Grouped wrap the internal intermediate topic name, key/value serde of new data after aggregation/reduce operation
     * - reduce -> aggregate operation, sum total shares of group (by symbol) data
     * Note:
     * - we use Builder pattern to make a copy of an object and update a field without modifying the origin object in stream processing
     */
    //rolling reduction
    KTable<String, ShareVolume> shareVolumeKTable =
        builder.stream(Topics.STOCK_TRANSACTIONS.topicName(),
                Consumed.with(Serdes.String(), stockTransactionSerde)
                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
            .mapValues(tx -> ShareVolume.newBuilder(tx).build())
            .groupBy((k, v) -> v.getSymbol(),
                Grouped.<String, ShareVolume>as("by-symbol")
                    .withKeySerde(Serdes.serdeFrom(String.class))
                    .withValueSerde(shareVolumeSerde
                    ))
            .reduce(ShareVolume::sum);

    /**
     * Top five aggregation (by sahre volume) summary
     * - group ShareVolume by industry
     * - start to add ShareVolume objects -> aggregation object is FixedSizePriorityQueue,
     * the queue keep only top 5 companies by share volume
     * - Map the queue into a string, reporting top-5 stocks per industry by share volume
     * - Write out the string result to a topic
     *
     * Aggregator note:
     * - initializer: a fixedSizeQueue to accumulate data
     * - aggregate adder: add new updates
     * - aggregate remover: remove old updates
     * - Materialized wrapping key/value serde
     */
    NumberFormat numberFormat = NumberFormat.getInstance();
    ValueMapper<FixedSizePriorityQueue, String> shareVolumeQueueToStringMapper = queue -> {
      StringBuilder sb = new StringBuilder();
      Iterator<ShareVolume> iterator = queue.iterator();
      int count = 1;
      while (iterator.hasNext()) {
        ShareVolume sv = iterator.next();
        if (sv != null) {
          sb.append(count++)
              .append(")")
              .append(sv.getSymbol())
              .append(":")
              .append(numberFormat.format(sv.getShares()))
              .append(" ");
        }
      }
      return sb.toString();
    };
    var comparator = new ShareVolumeComparator();
    var topFiveQueue = new FixedSizePriorityQueue(comparator, 5);
    Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.fixedSizePriorityQueueSerde();

    // rolling aggregation
    KTable<String, FixedSizePriorityQueue> topFiveKTable =
        shareVolumeKTable
            .groupBy(
                (k, v) -> KeyValue.pair(v.getIndustry(), v),
                Grouped.<String, ShareVolume>as("by-industry")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(shareVolumeSerde)

            ).aggregate(
                () -> topFiveQueue,
                (k, v, queue) -> queue.add(v),
                (k, v, queue) -> queue.remove(v),
                Materialized.with(Serdes.String(), fixedSizePriorityQueueSerde)
            );

    topFiveKTable.mapValues(shareVolumeQueueToStringMapper)
        .toStream()
        .peek((k, v) -> LOG.info("Stock volume by industry {} {}", k, v))
        .to(Topics.STOCK_VOLUME_BY_COMPANY.topicName(),
            Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  @Override
  public void mockData() {
    MockDataProducer.produceStockTransactions(
        15,
        50,
        25,
        false);
  }
}
