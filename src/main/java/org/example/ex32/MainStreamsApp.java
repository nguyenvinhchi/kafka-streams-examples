package org.example.ex32;

public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS basic - Mart tx basic stream processing");

        final var streams = new MartTransactionBasicStream();
        streams.start(args);
    }

}