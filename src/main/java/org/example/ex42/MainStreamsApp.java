package org.example.ex42;


public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS - Mart streams join streams - windowing join");

        final var streams = new MartWindowingJoinStream();
        streams.start(args);
    }

}