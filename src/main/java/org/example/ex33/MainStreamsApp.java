package org.example.ex33;

public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS basic - Mart tx advance stream processing");

        final var streams = new MartTransactionAdvanceStream();
        streams.start(args);
    }

}