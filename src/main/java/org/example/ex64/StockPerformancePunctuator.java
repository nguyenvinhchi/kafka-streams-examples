package org.example.ex64;

import java.util.Objects;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.StockPerformance;

public class StockPerformancePunctuator implements Punctuator {

  private final double differentialThreshold;
  private final ProcessorContext context;
  private final KeyValueStore<String, StockPerformance> keyValueStore;
  public StockPerformancePunctuator(double differentialThreshold,
      ProcessorContext context,
      KeyValueStore<String, StockPerformance> keyValueStore) {

    this.differentialThreshold = differentialThreshold;
    this.context = context;
    this.keyValueStore = keyValueStore;
  }
  @Override
  public void punctuate(long timestamp) {
    try (KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all()) {
      while (performanceIterator.hasNext()) {
        KeyValue<String, StockPerformance> kv = performanceIterator.next();
        String key = kv.key;
        var stockPerformance = kv.value;
        if (Objects.nonNull(stockPerformance)) {
          if (stockPerformance.priceDifferential() >= differentialThreshold ||
              stockPerformance.volumeDifferential() >= differentialThreshold) {
            context.forward(key, stockPerformance);
          }
        }
      }
    }
  }
}
