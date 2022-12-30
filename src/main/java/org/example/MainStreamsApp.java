package org.example;

import org.example.ex61.BeerPurchaseProcessorStream;
import org.example.ex62.StockPerformanceProcessorStream;
import org.example.ex63.CoGroupingProcessorStream;
import org.example.ex64.ProcessorApiAndKStreamApiStream;

public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS Hello world!");

        final var streams = new ProcessorApiAndKStreamApiStream();
        streams.start(args);

    }

}