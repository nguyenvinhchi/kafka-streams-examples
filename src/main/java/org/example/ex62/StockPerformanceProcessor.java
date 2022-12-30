package org.example.ex62;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.StockPerformance;
import org.example.base.stock.model.StockTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockPerformanceProcessor implements Processor<String, StockTransaction, String, StockPerformance> {

  static Logger LOG = LoggerFactory.getLogger(StockPerformanceProcessor.class);
  private final String stockStateStoreName;
  private double differentialThreshold;

  private KeyValueStore<String, StockPerformance> keyValueStore;

  public StockPerformanceProcessor(String stockStateStoreName, double differentialThreshold) {

    this.stockStateStoreName = stockStateStoreName;
    this.differentialThreshold = differentialThreshold;
  }

  @Override
  public void init(ProcessorContext<String, StockPerformance> context) {
    keyValueStore = context.getStateStore(this.stockStateStoreName);
    StockPerformancePunctuator punctuator = new StockPerformancePunctuator(
        differentialThreshold,
        context,
        keyValueStore);

    /**
     * schedule every 10 seconds, puntuator() will be execute
     */
    context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, punctuator);
  }

  @Override
  public void process(Record<String, StockTransaction> record) {
    StockTransaction stockTransaction = record.value();
    if (stockTransaction == null) {
      return;
    }

//    LOG.info("[stock-transaction]: {}", stockTransaction);

    var symbol = stockTransaction.getSymbol();
    if (symbol != null) {
      StockPerformance stockPerformance = keyValueStore.get(symbol);
      if (stockPerformance == null) {
        stockPerformance = new StockPerformance();
      }
      stockPerformance.updatePriceStats(stockTransaction.getSharePrice());
      stockPerformance.updateVolumeStats(stockTransaction.getShares());
      stockPerformance.setLastUpdateSent(Instant.now());

      keyValueStore.put(symbol, stockPerformance);
    }
  }

  @Override
  public void close() {
  }
}
