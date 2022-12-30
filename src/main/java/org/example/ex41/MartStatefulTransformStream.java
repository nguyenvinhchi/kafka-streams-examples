package org.example.ex41;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.base.BaseStreamsApp;
import org.example.base.zmart.data.MockDataProducer;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.PurchasePattern;
import org.example.base.zmart.model.RewardAccumulator;
import org.example.base.zmart.serdes.StreamsSerdes;
import org.example.ex33.Topics;
import org.example.ex41.transformer.PurchaseRewardTransformer;

public class MartStatefulTransformStream extends BaseStreamsApp {

  @Override
  public Topology createTopology() {
    Serde<Purchase> purchaseSerde = StreamsSerdes.purchaseSerde();
    Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde();
    Serde<String> stringSerde = Serdes.String();

    var b = new StreamsBuilder();

    // Source purchase stream
    KStream<String, Purchase> purchaseStream =
        b.stream(
                Topics.MART_PURCHASES.value(),
                Consumed.with(stringSerde, purchaseSerde)
            )
            .mapValues(
                p -> Purchase.builder(p).maskCreditCard().build(),
                Named.as("Purchase-source"));

    // stateful transform - update reward points into RewardAccumulator
    // what kind of state store you want + key/value serdes
    KeyValueBytesStoreSupplier storeSupplier =
        Stores.inMemoryKeyValueStore(StoreNames.REWARDS_STATE_STORE.value());
    StoreBuilder<KeyValueStore<String, Integer>> storeBuilder =
        Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
    b.addStateStore(storeBuilder);
    // transform to record with new key - customerID
    KStream<String, Purchase> txByCustomerStream =
        purchaseStream.selectKey(
            (k, v) -> v.getCustomerId(), Named.as("Transaction-by-customer"));
    // transform value - Purchase to RewardAccumulator
    // & calc points based on state store
    KStream<String, RewardAccumulator> rewardAccumulatorStream =
        txByCustomerStream.transformValues(
            () -> new PurchaseRewardTransformer(StoreNames.REWARDS_STATE_STORE.value()),
            Named.as("Reward-accumulator-transformer"),
            StoreNames.REWARDS_STATE_STORE.value());
    rewardAccumulatorStream.print(Printed.<String, RewardAccumulator>toFile(
            "/Users/chi/IdeaProjects/ksdemo/logs/rewards.log").withLabel("rewards"));
    rewardAccumulatorStream.to(Topics.MART_REWARDS.value(),
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
