package org.example.base.stock.serdes;


import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.ArrayDeque;

public class DoubleDequeDeserializer implements JsonDeserializer<ArrayDeque<Double>> {

  @Override
  public ArrayDeque deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    if (json == null) {
      return null;
    }
    var queue = new ArrayDeque<Double>();
    JsonArray array = json.getAsJsonArray();
    for (var item : array) {
      var d = item.getAsDouble();
      queue.add(d);
    }
    return queue;
  }
}
