package org.example.ex33;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.base.BaseStreamsApp;
import org.example.base.zmart.data.MockDataProducer;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.PurchasePattern;
import org.example.base.zmart.model.RewardAccumulator;
import org.example.base.zmart.serdes.StreamsSerdes;
import org.example.base.zmart.service.SecurityDBService;

public class MartTransactionAdvanceStream extends BaseStreamsApp {

  public static final double LOWEST_PRICE = 5.0;
  public static final String SUSPICIOUS_EMP_ID = "000000";

  @Override
  public Topology createTopology() {
    Serde<Purchase> purchaseSerde = StreamsSerdes.purchaseSerde();
    Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.purchasePatternSerde();
    Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.rewardAccumulatorSerde();
    Serde<String> stringSerde = Serdes.String();

    var b = new StreamsBuilder();

    // Source stream
    KStream<String, Purchase> purchaseStream =
        b.stream(
                Topics.MART_PURCHASES.value(),
                Consumed.with(stringSerde, purchaseSerde)
            )
            .mapValues(
                p -> Purchase.builder(p).maskCreditCard().build(),
                Named.as("Purchase-source"));

    // Purchase Patterns
    KStream<String, PurchasePattern> patternStream =
        purchaseStream.mapValues(
            p -> PurchasePattern.builder(p).build(),
            Named.as("Patterns"));
    patternStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
    patternStream.to(
        Topics.MART_PATTERNS.value(), Produced.with(stringSerde, purchasePatternSerde));

    // Purchase Rewards
    KStream<String, RewardAccumulator> rewardStream =
        purchaseStream.mapValues(
            p -> RewardAccumulator.builder(p).build(),
            Named.as("Rewards"));
    rewardStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
    rewardStream.to(
        Topics.MART_REWARDS.value(), Produced.with(stringSerde, rewardAccumulatorSerde));

    // Filter out low price tx, change key (repartition)
    KeyValueMapper<String, Purchase, Long> purchaseDateAsKey =
        (key, purchase) -> purchase.getPurchaseDate().getTime();
    KStream<Long, Purchase> priceFilteredStream =
        purchaseStream.filter(
                (key, purchase) -> purchase.getPrice() > LOWEST_PRICE,
                Named.as("Price-filter"))
            .selectKey(purchaseDateAsKey);
    priceFilteredStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
    priceFilteredStream.to(
        Topics.MART_TRANSACTIONS.value(),
        Produced.with(Serdes.Long(), purchaseSerde));

    // branching stream for separating out purchases in new departments to their own topics
    Predicate<String, Purchase> isCoffee =
        (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
    Predicate<String, Purchase> isElectronics =
        (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

    purchaseStream.split(Named.as("Purchase-store-branch"))
        .branch(
            isCoffee,
            Branched
                .withConsumer(ks -> {
                  ks.print(Printed.<String, Purchase>toSysOut().withLabel("coffee"));
                  ks.to(
                      Topics.MART_COFFEE.value(),
                      Produced.with(stringSerde, purchaseSerde));
                }, "-coffee")
        ).branch(
            isElectronics,
            Branched.as("electronics-branch")
                .withConsumer(ks -> {
                  ks.print(Printed.<String, Purchase>toSysOut().withLabel("-electronics"));
                  ks.to(
                      Topics.MART_ELECTRONICS.value(),
                      Produced.with(stringSerde, purchaseSerde)
                  );
                }, "electronics")
        );

    // security Requirements to record transactions for certain employee
    ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
        SecurityDBService.saveRecord(
            purchase.getPurchaseDate(),
            purchase.getEmployeeId(),
            purchase.getItemPurchased());
    purchaseStream.filter(
            (key, purchase) -> purchase.getEmployeeId().equals(SUSPICIOUS_EMP_ID),
            Named.as("Security-suspicious"))
        .foreach(purchaseForeachAction);

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
