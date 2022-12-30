package org.example.ex41.transformer;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.base.zmart.model.Purchase;
import org.example.base.zmart.model.RewardAccumulator;

import java.util.Objects;

public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {
    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public PurchaseRewardTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = this.context.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase value) {
        RewardAccumulator reward = RewardAccumulator.builder(value).build();
        // Get customer current reward points
        Integer accumulatedSoFar = stateStore.get(reward.getCustomerId());

        // Update customer reward points
        if (accumulatedSoFar != null) {
            reward.addRewardPoints(accumulatedSoFar);
        }

        // Update state store for customer reward
        stateStore.put(reward.getCustomerId(), reward.getTotalRewardPoints());

        return reward;
    }

    @Override
    public void close() {
        //no-op
    }
}
