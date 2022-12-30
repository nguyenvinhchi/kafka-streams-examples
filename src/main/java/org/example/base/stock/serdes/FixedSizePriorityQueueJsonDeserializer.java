package org.example.base.stock.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.base.stock.util.FixedSizePriorityQueue;

public class FixedSizePriorityQueueJsonDeserializer implements Deserializer<FixedSizePriorityQueue> {

  private Gson gson;
  private FixedSizePriorityQueue deserializedClass;
  private Type reflectionTypeToken;

  public FixedSizePriorityQueueJsonDeserializer() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
    gson = builder.create();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> map, boolean b) {
    if(deserializedClass == null) {
      deserializedClass = (FixedSizePriorityQueue) map.get("serializedClass");
    }
  }

  @Override
  public FixedSizePriorityQueue deserialize(String s, byte[] bytes) {
    if(bytes == null){
      return null;
    }

    return gson.fromJson(new String(bytes),FixedSizePriorityQueue.class);

  }

  @Override
  public void close() {

  }
}
