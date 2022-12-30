package org.example.ex63;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.base.stock.model.ClickEvent;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.util.Tuple;

public class StockTransactionProcessor implements
    Processor<String, StockTransaction, String, StockTransaction> {
  private ProcessorContext<String, StockTransaction> context;

  @Override
  public void init(ProcessorContext<String, StockTransaction> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, StockTransaction> record) {
    String key = record.key();

    // forward clickEvent tuple
    if (Objects.nonNull(key)) {
      Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(null, record.value());
      context.forward(new Record(key, tuple, System.currentTimeMillis()));
    }
  }

  @Override
  public void close() {
    // intentionally blank
  }
}
