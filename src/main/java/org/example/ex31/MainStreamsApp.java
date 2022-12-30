package org.example.ex31;

public class MainStreamsApp {

    public static void main(String[] args) {
        System.out.println("KS basic - upper case transform");

        final var streams = new BasicUpperCaseStream();
        streams.start(args);

    }

}