package org.example.ex63;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.base.stock.model.ClickEvent;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.util.Tuple;

public class ClickEventProcessor implements
    Processor<String, ClickEvent, String, ClickEvent> {
  private ProcessorContext<String, ClickEvent> context;

  @Override
  public void init(ProcessorContext<String, ClickEvent> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, ClickEvent> record) {
    String key = record.key();

    // forward clickEvent tuple
    if (Objects.nonNull(key)) {
      Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(record.value(), null);
      context.forward(new Record(key, tuple, System.currentTimeMillis()));
    }
  }

  @Override
  public void close() {
    // intentionally blank
  }
}
