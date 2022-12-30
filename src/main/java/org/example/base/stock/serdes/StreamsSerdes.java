package org.example.base.stock.serdes;

import com.google.gson.reflect.TypeToken;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.example.base.stock.model.TransactionSummary;
import org.example.base.stock.util.FixedSizePriorityQueue;
import org.example.base.stock.model.ClickEvent;
import org.example.base.stock.model.ShareVolume;
import org.example.base.stock.model.StockPerformance;
import org.example.base.stock.model.StockTickerData;
import org.example.base.stock.model.StockTransaction;
import org.example.base.stock.util.Tuple;

public class StreamsSerdes {
    public static Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde() {
        return new FixedSizePriorityQueueSerde();
    }
    public static Serde<ShareVolume> shareVolumeSerde() {
        return new JsonSerde<>(ShareVolume.class);
    }
    public static Serde<StockTransaction> stockTransactionSerde() {
        return new JsonSerde<>(StockTransaction.class);
    }
    public static Serde<StockTickerData> stockTickerSerde() {
        return new JsonSerde<>(StockTickerData.class);
    }

    public static Serde<TransactionSummary> transactionSummarySerde() {
        return new JsonSerde<>(TransactionSummary.class);
    }

    public static Serde<StockPerformance> stockPerformanceSerde() {
        return new JsonSerde<>(StockPerformance.class);
    }

    public static Serde<ClickEvent> clickEventSerde() {
        return new JsonSerde<>(ClickEvent.class);
    }

    public static Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> eventPerformanceTupleSerde() {
        var tupleType = new TypeToken<Tuple<List<ClickEvent>, List<StockTransaction>>>(){}.getType();
        return new JsonSerde<>(tupleType);
    }
}
