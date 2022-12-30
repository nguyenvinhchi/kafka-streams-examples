package org.example.ex62;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.StockPerformance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockPerformancePunctuator implements Punctuator {

  static Logger LOG = LoggerFactory.getLogger(StockPerformancePunctuator.class);
  private double differentialThreshold;
  private ProcessorContext<String, StockPerformance> context;
  private KeyValueStore<String, StockPerformance> keyValueStore;

  public StockPerformancePunctuator(
      double differentialThreshold,
      ProcessorContext<String, StockPerformance> context,
      KeyValueStore<String, StockPerformance> keyValueStore
  ) {
    this.differentialThreshold = differentialThreshold;
    this.context = context;
    this.keyValueStore = keyValueStore;
  }

  @Override
  public void punctuate(long timestamp) {
    LOG.info("[Puntuation]");
    try (KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();){
      while (performanceIterator.hasNext()) {
        KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
        String key = keyValue.key;
        StockPerformance stockPerformance = keyValue.value;

        // only forward interested records when they match some condition
        if (stockPerformance != null &&
            (stockPerformance.priceDifferential() >= differentialThreshold ||
                stockPerformance.volumeDifferential() >= differentialThreshold)
        ) {
          context.forward(new Record<>(key, stockPerformance, System.currentTimeMillis()));
        }
      }
    }
  }
}
