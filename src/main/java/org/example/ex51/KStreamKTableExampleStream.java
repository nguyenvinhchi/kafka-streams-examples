package org.example.ex51;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.example.base.BaseStreamsApp;
import org.example.base.stock.data.MockDataProducer;
import org.example.base.stock.model.StockTickerData;
import org.example.base.stock.serdes.StreamsSerdes;

import static org.example.base.stock.data.MockDataProducer.STOCK_TICKER_STREAM_TOPIC;
import static org.example.base.stock.data.MockDataProducer.STOCK_TICKER_TABLE_TOPIC;

public class KStreamKTableExampleStream extends BaseStreamsApp {
    @Override
    public Topology createTopology() {
        var stringSerde = Serdes.String();
        var stockTickerSerde = StreamsSerdes.stockTickerSerde();

        var builder = new StreamsBuilder();
        KTable<String, StockTickerData> stockTickerTable = builder
                .table(STOCK_TICKER_TABLE_TOPIC,
                        Consumed.with(stringSerde, stockTickerSerde));

        KStream<String, StockTickerData> stockTickerStream = builder
                .stream(STOCK_TICKER_STREAM_TOPIC);

        stockTickerTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KTable"));
        stockTickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel( "Stocks-KStream"));

        return builder.build();
    }

    @Override
    public void mockData() {
        // mock data
        int numberCompanies = 3;
        int iterations = 3;
        MockDataProducer.produceStockTickerData(numberCompanies, iterations);
    }
}
