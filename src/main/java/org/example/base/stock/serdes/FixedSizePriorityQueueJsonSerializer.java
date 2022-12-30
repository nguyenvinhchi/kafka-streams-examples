package org.example.base.stock.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.example.base.stock.util.FixedSizePriorityQueue;

public class FixedSizePriorityQueueJsonSerializer implements Serializer<FixedSizePriorityQueue> {

  private Gson gson;

  public FixedSizePriorityQueueJsonSerializer() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
    gson = builder.create();
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String topic, FixedSizePriorityQueue t) {
    return gson.toJson(t).getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public void close() {

  }
}
