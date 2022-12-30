package org.example.ex64;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.StockPerformance;
import org.example.base.stock.model.StockTransaction;

public class StockPerformanceTransformer implements
    Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> {

  private final String stateStoreName;
  private final double differentialThreshold;

  private KeyValueStore<String, StockPerformance> keyValueStore;

  public StockPerformanceTransformer(String stateStoreName, double differentialThreshold) {
    this.stateStoreName = stateStoreName;
    this.differentialThreshold = differentialThreshold;
  }

  @Override
  public void init(ProcessorContext context) {
    keyValueStore = context.getStateStore(stateStoreName);
    var punctuator = new StockPerformancePunctuator(differentialThreshold, context, keyValueStore);

    // Every 15 seconds, forward matching business records to downstream nodes
    context.schedule(Duration.ofSeconds(15), PunctuationType.STREAM_TIME, punctuator);
  }

  @Override
  public KeyValue<String, StockPerformance> transform(String key, StockTransaction value) {

    // Update state store with transformed value
    if (Objects.nonNull(key)) {
      StockPerformance stockPerformance = keyValueStore.get(key);
      if (Objects.isNull(stockPerformance)) {
        stockPerformance = StockPerformance.newBuilder().build();
      }

      stockPerformance.updatePriceStats(value.getSharePrice());
      stockPerformance.updateVolumeStats(value.getShares());
      stockPerformance.setLastUpdateSent(Instant.now());

      keyValueStore.put(key, stockPerformance);
    }
    return null;
  }

  @Override
  public void close() {
    // no-op
  }
}
