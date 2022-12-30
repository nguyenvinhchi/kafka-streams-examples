package org.example.ex61;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.example.base.BaseStreamsApp;
import org.example.base.beersales.Topics;
import org.example.base.beersales.data.MockDataProducer;
import org.example.base.beersales.model.BeerPurchase;
import org.example.base.beersales.serde.StreamSerdes;

public class BeerPurchaseProcessorStream extends BaseStreamsApp {

  static final String beerPurchaseSource = "beer-purchase-source";
  static final String purchaseProcessorName = "purchase-processor";
  static final String domesticSalesSink = "domestic-sales-sink";
  static final String internationalSalesSink = "international-sales-sink";

  @Override
  public Topology createTopology() {
    var topology = new Topology();

    var keySerde = Serdes.String();
    var beerPurchaseSerde = StreamSerdes.beerPurchaseSerde();

    topology.addSource(
        AutoOffsetReset.LATEST,
        beerPurchaseSource,
        keySerde.deserializer(),
        beerPurchaseSerde.deserializer(),
        Topics.BEER_PURCHASE.topicName()
    );

    ProcessorSupplier<String, BeerPurchase, String, BeerPurchase> processorSupplier =
        () -> new BeerPurchaseProcessor(domesticSalesSink, internationalSalesSink);

    topology.addProcessor(
        purchaseProcessorName,
        processorSupplier,
        beerPurchaseSource);

    topology.addSink(
        internationalSalesSink,
        Topics.BEER_INTERNATIONAL_SALES.topicName(),
        keySerde.serializer(),
        beerPurchaseSerde.serializer(),
        purchaseProcessorName
    );

    topology.addSink(
        domesticSalesSink,
        Topics.BEER_DOMESTIC_SALES.topicName(),
        keySerde.serializer(),
        beerPurchaseSerde.serializer(),
        purchaseProcessorName
    );

    return topology;
  }

  @Override
  public void mockData() {
    MockDataProducer.produceBeerPurchases(5);
  }
}
