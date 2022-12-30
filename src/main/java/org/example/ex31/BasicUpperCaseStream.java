package org.example.ex31;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.base.BaseStreamsApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicUpperCaseStream extends BaseStreamsApp {
    private static final Logger LOG = LoggerFactory.getLogger(BasicUpperCaseStream.class);
    public Topology createTopology() {
        var builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(
            Topics.WORDS.topicName(),
            Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> upperCase = source.mapValues(s -> s.toUpperCase());
        upperCase.to(
            Topics.WORDS_TRANSFORMED.topicName(),
            Produced.with(Serdes.String(), Serdes.String())
        );

        // for demonstration only - never use in prod
        upperCase.print(Printed.<String, String>toSysOut().withLabel("UPPERCASE WORDS"));

        return builder.build();
    }
}
