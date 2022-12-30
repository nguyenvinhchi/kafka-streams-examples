package org.example.base.stock.serdes;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import org.example.base.stock.model.StockPerformance;

// This deserializing is not working correctly with Gson error: json document is not fully consumed
public class StockPerformanceTypeAdapter extends TypeAdapter<StockPerformance> {

  @Override
  public void write(JsonWriter writer, StockPerformance stockPerformance) throws IOException {
    writer.beginObject();
    writer.name("last_update_sent");
    Instant lastUpdateSent = stockPerformance.getLastUpdateSent();
    writer.value(lastUpdateSent == null ? null : lastUpdateSent.toEpochMilli());

    writer.name("current_price");
    writer.value(stockPerformance.getCurrentPrice());

    writer.name("price_differential");
    writer.value(stockPerformance.priceDifferential());

    writer.name("share_differential");
    writer.value(stockPerformance.volumeDifferential());

    writer.name("current_share_volume");
    writer.value(stockPerformance.getCurrentShareVolume());

    writer.name("current_average_price");
    writer.value(stockPerformance.getCurrentAveragePrice());

    writer.name("current_average_volume");
    writer.value(stockPerformance.getCurrentAverageVolume());

    writer.name("share_volume_lookback");
    writer.beginArray();
    for (var item : stockPerformance.shareVolumeLookback()) {
      writer.value(item);
    }
    writer.endArray();

    writer.name("share_price_lookback");
    writer.beginArray();
    for (var item : stockPerformance.sharePriceLookback()) {
      writer.value(item);
    }
    writer.endArray();

    writer.endObject();
  }

  @Override
  public StockPerformance read(JsonReader reader) throws IOException {
    reader.beginObject();
    var builder = StockPerformance.newBuilder();
    String name = null;
    while (reader.hasNext()) {
      var token = reader.peek();
      if (token.equals(JsonToken.NAME)) {
        name = reader.nextName();
      }

      switch(name) {
        case "last_update_sent":
          Long timeMs = reader.nextLong();
          if (timeMs != null) {
            builder.withLastUpdateSent(Instant.ofEpochMilli(timeMs));
          }
          break;

        case "current_price":
          double currentPrice = reader.nextDouble();
          builder.withCurrentPrice(currentPrice);
          break;

        case "price_differential":
          double priceDifferential = reader.nextDouble();
          builder.withPriceDifferential(priceDifferential);
          break;

        case "share_differential":
          var shareDifferential = reader.nextDouble();
          builder.withShareDifferential(shareDifferential);
          break;

        case "current_share_volume":
          var currentShareVolume = reader.nextInt();
          builder.withCurrentShareVolume(currentShareVolume);
          break;

        case "current_average_price":
          var currentAveragePrice = reader.nextDouble();
          builder.withCurrentAveragePrice(currentAveragePrice);
          break;

        case "current_average_volume":
          var currentAverageVolume = reader.nextDouble();
          builder.withCurrentAverageVolume(currentAverageVolume);
          break;

        case "share_volume_lookback":
          reader.beginArray();
          var shareVolumeLookbackList = new ArrayDeque<Double>();
          while (reader.hasNext()) {
            shareVolumeLookbackList.add(reader.nextDouble());
          }
          reader.endArray();
          builder.withShareVolumeLookback(shareVolumeLookbackList);
          break;

        case "share_price_lookback":
          reader.beginArray();
          var sharePriceLookbackList = new ArrayDeque<Double>();
          while (reader.hasNext()) {
            sharePriceLookbackList.add(reader.nextDouble());
          }
          reader.endArray();
          builder.withSharePriceLookback(sharePriceLookbackList);
          break;
      }

      if ("share_price_lookback".equals(name)) {
        System.out.println(reader.peek());
        reader.endObject();
        System.out.println(reader.peek());
        break;
      }
    }

    return builder.build();
  }
}
