package org.example.ex32;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.base.BaseStreamsApp;
import org.example.base.zmart.data.MockDataProducer;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.PurchasePattern;
import org.example.base.zmart.model.RewardAccumulator;
import org.example.base.zmart.serdes.StreamsSerdes;

public class MartTransactionBasicStream extends BaseStreamsApp {

  @Override
  public Topology createTopology() {
    Serde<Purchase> purchaseSerde = StreamsSerdes.purchaseSerde();
    Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.purchasePatternSerde();
    Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde();
    Serde<String> stringSerde = Serdes.String();

    var b = new StreamsBuilder();
    // source stream & masking credit card number
    KStream<String, Purchase> purchaseStream =
        b.stream(
                Topics.MART_PURCHASES.topicName(),
                Consumed.with(stringSerde, purchaseSerde))
            .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
    purchaseStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchases"));
    purchaseStream.to(
        Topics.MART_TRANSACTIONS.topicName(),
        Produced.with(stringSerde, purchaseSerde));

    // Purchase Patterns
    KStream<String, PurchasePattern> patternStream =
        purchaseStream.mapValues(p -> PurchasePattern.builder(p).build());
    patternStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
    patternStream.to(
        Topics.MART_PATTERNS.topicName(),
        Produced.with(stringSerde, purchasePatternSerde));

    // Purchase Rewards
    KStream<String, RewardAccumulator> rewardStream =
        purchaseStream.mapValues(p -> RewardAccumulator.builder(p).build());
    rewardStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
    rewardStream.to(
        Topics.MART_REWARDS.topicName(),
        Produced.with(stringSerde, rewardAccumulatorSerde));

    return b.build();
  }

  @Override
  public void start(String[] args) {
    super.start(args);
    MockDataProducer.producePurchaseData();
  }

  @Override
  public void close() {
    MockDataProducer.shutdown();
  }
}
