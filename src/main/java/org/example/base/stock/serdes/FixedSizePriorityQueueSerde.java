package org.example.base.stock.serdes;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.example.base.stock.util.FixedSizePriorityQueue;

public class FixedSizePriorityQueueSerde extends WrapperSerde<FixedSizePriorityQueue> {
  public FixedSizePriorityQueueSerde() {
    super(new FixedSizePriorityQueueJsonSerializer(),
        new FixedSizePriorityQueueJsonDeserializer());
  }
}
