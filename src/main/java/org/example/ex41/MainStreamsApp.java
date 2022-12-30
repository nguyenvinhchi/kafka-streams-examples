package org.example.ex41;


public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS - Mart stateful transform stream processing");

        final var streams = new MartStatefulTransformStream();
        streams.start(args);
    }

}