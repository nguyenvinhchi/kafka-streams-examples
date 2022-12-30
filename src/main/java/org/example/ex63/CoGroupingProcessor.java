package org.example.ex63;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.ClickEvent;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.util.Tuple;

public class CoGroupingProcessor implements
    Processor<String, Tuple<ClickEvent, StockTransaction>,
        String, Tuple<List<ClickEvent>, List<StockTransaction>>> {

  public static final String TUPLE_COGROUP_STORE_NAME = "tuple-cogrouped-store";
  private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleCoGroupStore;

  @Override
  public void init(
      ProcessorContext<String, Tuple<List<ClickEvent>, List<StockTransaction>>> context) {
    this.tupleCoGroupStore = context.getStateStore(TUPLE_COGROUP_STORE_NAME);
    var punctuator = new CogroupingPunctuator(tupleCoGroupStore, context);
    context.schedule(Duration.ofSeconds(15), PunctuationType.STREAM_TIME, punctuator);
  }

  @Override
  public void process(Record<String, Tuple<ClickEvent, StockTransaction>> record) {
    var key = record.key();
    Tuple<List<ClickEvent>, List<StockTransaction>> cogroupedTuple = tupleCoGroupStore.get(key);
    if (Objects.isNull(cogroupedTuple)) {
      cogroupedTuple = Tuple.of(new ArrayList<>(), new ArrayList<>());
    }
    var value = record.value();

    if (Objects.nonNull(value._1)) {
      cogroupedTuple._1.add(value._1);
    }

    if (Objects.nonNull(value._2)) {
      cogroupedTuple._2.add(value._2);
    }

    tupleCoGroupStore.put(key, cogroupedTuple);
  }

  @Override
  public void close() {
    // intentionally blank
  }
}
