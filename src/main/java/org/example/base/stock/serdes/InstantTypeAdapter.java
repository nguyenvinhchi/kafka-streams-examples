package org.example.base.stock.serdes;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.Instant;

/**
 * alternative to InstantSerializer + InstantDeserializer, we use TypeAdapter
 */
public class InstantTypeAdapter extends TypeAdapter<Instant> {

  @Override
  public void write(JsonWriter writer, Instant value) throws IOException {
    long v = value.toEpochMilli();
    writer.value(String.valueOf(v));
  }

  @Override
  public Instant read(JsonReader reader) throws IOException {
    String v = reader.nextString();

    return Instant.ofEpochMilli(Long.parseLong(v));
  }
}
