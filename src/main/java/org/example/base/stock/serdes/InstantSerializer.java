package org.example.base.stock.serdes;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class InstantSerializer implements JsonSerializer<Instant> {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

  @Override
  public JsonElement serialize(Instant src, Type typeOfSrc, JsonSerializationContext context) {
    if (src == null) {
      return null;
    }
    return new JsonPrimitive(FORMATTER.format(src));
  }
}
