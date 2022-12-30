package org.example.ex42.joiner;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.base.zmart.model.CorrelatedPurchase;
import org.example.base.zmart.model.Purchase;

import java.util.ArrayList;
import java.util.List;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {

    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {
        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();
        var purchaseDate = purchase != null ? purchase.getPurchaseDate() : null;
        var price = purchase != null ? purchase.getPrice() : 0.0;
        var itemPurchased = purchase != null ? purchase.getItemPurchased() : null;
        var otherPurchaseDate = otherPurchase != null ? otherPurchase.getPurchaseDate() : null;
        var otherPrice = otherPurchase != null ? otherPurchase.getPrice() : null;
        var otherItemPurchase = otherPurchase != null ? otherPurchase.getItemPurchased() : null;

        List<String> purchasedItems = new ArrayList<>();
        if (itemPurchased != null) {
            purchasedItems.add(itemPurchased);
        }
        if (otherItemPurchase != null) {
            purchasedItems.add(otherItemPurchase);
        }

        String customerId = purchase != null ? purchase.getCustomerId() : null;
        String otherCustomerId = otherPurchase != null ? otherPurchase.getCustomerId() : null;

        builder.withCustomerId(customerId != null ? customerId : otherCustomerId)
                .withFirstPurchaseDate(purchaseDate)
                .withSecondPurchaseDate(otherPurchaseDate)
                .withItemsPurchased(purchasedItems)
                .withTotalAmount(price + otherPrice);
        return builder.build();
    }
}
