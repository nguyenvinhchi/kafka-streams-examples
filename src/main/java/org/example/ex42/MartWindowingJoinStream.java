package org.example.ex42;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.base.BaseStreamsApp;
import org.example.ex33.Topics;
import org.example.ex42.joiner.PurchaseJoiner;
import org.example.base.zmart.data.MockDataProducer;
import org.example.base.zmart.model.CorrelatedPurchase;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.RewardAccumulator;
import org.example.base.zmart.serdes.StreamsSerdes;
import org.example.ex41.transformer.PurchaseRewardTransformer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Keep traffic in electronics store high by offering coupons for the coffee store
 * to identify customers who have bought coffee and made a purchase in the electronics store
 * and give them coupons almost immediately after their second transaction
 *
 * Join sales from coffee with sales from electronics in windows of 20-minute interval
 * (the windowing adding the logic that customer buy coffee also buy electronics)
 */
public class MartWindowingJoinStream extends BaseStreamsApp {

    @Override
    public Topology createTopology() {
        Serde<Purchase> purchaseSerde = StreamsSerdes.purchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        var b = new StreamsBuilder();

        // Source stream purchase transactions
        KStream<String, Purchase> purchaseStream =
                b.stream(Topics.MART_PURCHASES.value(), Consumed.with(stringSerde, purchaseSerde))
                        .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        // transform to record with new key - customerID
        KStream<String, Purchase> txByCustomerStream = purchaseStream
                .selectKey((k, v) -> v.getCustomerId(), Named.as("tx-by-customer"))
                .repartition(Repartitioned.with(stringSerde, purchaseSerde).withName("tx-by-customer-repartition"));

        // Separating out purchases in their own department
        Predicate<String, Purchase> isCoffee =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");
        KStream<String, Purchase> coffeeStream =
            txByCustomerStream.filter(isCoffee, Named.as("Coffee-purchase"));
        KStream<String, Purchase> electronicsStream =
            txByCustomerStream.filter(isElectronics, Named.as("Electronics-purchase"));

        // Join coffee vs electronics
        // records to join must have timestamp within 20 min of each other
        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMinuteWindow = JoinWindows.of(Duration.ofMinutes(20));
        // Stream-Stream join using window state store
        KStream<String, CorrelatedPurchase> joinedStream = coffeeStream.join(
                electronicsStream,
                purchaseJoiner,
                twentyMinuteWindow,
                StreamJoined.with(stringSerde, purchaseSerde, purchaseSerde)
                    .withName("coffee-join-electronics"));
        joinedStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("bought-coffee-and-electronics"));

        return b.build();
    }

    private static Map<String, String> getChangeLogConfig() {
        Map<String, String> changeLogConfig = new HashMap<>();
        changeLogConfig.put("retention.ms", "172800000");
        changeLogConfig.put("retention.bytes", "10000000000");
        changeLogConfig.put("cleanup.policy", "compact,delete");
        return changeLogConfig;
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
