package org.example.base.beersales.serde;

import org.apache.kafka.common.serialization.Serde;
import org.example.base.beersales.model.BeerPurchase;
import org.example.base.stock.serdes.JsonSerde;

public class StreamSerdes {
  public static Serde<BeerPurchase> beerPurchaseSerde() {
    return new JsonSerde<>(BeerPurchase.class);
  }
}
