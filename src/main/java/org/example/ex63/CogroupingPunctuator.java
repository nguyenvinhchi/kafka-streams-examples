package org.example.ex63;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.stock.model.ClickEvent;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.util.Tuple;

public class CogroupingPunctuator implements Punctuator {

  private final KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore;
  private final ProcessorContext<String, Tuple<List<ClickEvent>, List<StockTransaction>>> context;

  public CogroupingPunctuator(
      KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore,
      ProcessorContext<String, Tuple<List<ClickEvent>, List<StockTransaction>>> context) {
    this.tupleStore = tupleStore;
    this.context = context;
  }

  @Override
  public void punctuate(long timestamp) {
    try(KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator =
        tupleStore.all()) {
      while (iterator.hasNext()) {
        KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouped = iterator.next();
        if (Objects.nonNull(cogrouped.value) && (!cogrouped.value._1.isEmpty()
            || !cogrouped.value._2.isEmpty())) {
          var clickEvents = new ArrayList<>(cogrouped.value._1);
          var stockTransactions = new ArrayList<>(cogrouped.value._2);
          var record = new Record<String, Tuple<List<ClickEvent>, List<StockTransaction>>>(
              cogrouped.key,
              Tuple.of(clickEvents, stockTransactions),
              System.currentTimeMillis());
          context.forward(record);

          // empty out current cogrouped results
          cogrouped.value._1.clear();
          cogrouped.value._2.clear();
          tupleStore.put(cogrouped.key, cogrouped.value);
        }
      }
    }
  }
}
