package org.example.base.zmart.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.PurchasePattern;
import org.example.base.zmart.model.RewardAccumulator;

public class StreamsSerdes {

    public static Serde<RewardAccumulator> rewardAccumulatorSerde() {
        return new JsonSerde<>(RewardAccumulator.class);
    }
    public static Serde<PurchasePattern> purchasePatternSerde() {
        return new JsonSerde<>(PurchasePattern.class);
    }
    public static Serde<Purchase> purchaseSerde() {
        return new JsonSerde<>(Purchase.class);
    }
}
