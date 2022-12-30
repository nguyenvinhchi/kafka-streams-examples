package org.example.base.stock.serdes;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.ArrayDeque;

public class DoubleDequeSerializer implements JsonSerializer<ArrayDeque<Double>> {

  @Override
  public JsonElement serialize(ArrayDeque<Double> src, Type typeOfSrc,
      JsonSerializationContext context) {
    if (src == null) {
      return null;
    }
    var array = new JsonArray();
    for (Double item : src) {
      array.add(item);
    }
    return array;
  }
}
