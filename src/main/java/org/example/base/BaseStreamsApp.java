package org.example.base;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class BaseStreamsApp {
    private static final Logger LOG = LoggerFactory.getLogger(BaseStreamsApp.class);

    public abstract Topology createTopology();

    public void start(final String[] args) {
        // Define the processing topology
        String name = this.getClass().getSimpleName();
        var topology = createTopology();

        var streamsConfiguration = getConfig();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.setStateListener((cur, old) -> {
                LOG.debug("Stream {} status change: {} -> {}",
                        name, old, cur);
            if (cur == State.RUNNING) {
                LOG.info("{}", topology.describe());
            }

            if (cur == State.REBALANCING) {
                LOG.info("Application is entering REBALANCING phase");
            }
        });
        // handle exception
        streams.setUncaughtExceptionHandler((t, e) -> {
            LOG.error("had exception ", e);
        });

        // Delete the application's local state
        streams.cleanUp();
        LOG.debug("Starting streams: {}", name);

        streams.start();
        mockData();
        LOG.debug("Streams running: {}", name);

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.debug("Shutting down streams: {}", name);
            streams.close();
            close();
        }));
    }

    public Properties getConfig() {
        var props = new Properties();
        String bootstrapServers = "localhost:9092";
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ksdemo_client_id");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksdemo_app");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ksdemo_group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                "/tmp/kafka-streams-global-tables"
        );
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 3 * 1024 * 1024);
        // Read the topic from the very beginning if no previous consumer offsets are found for this app.
        // Resetting an app will set any existing consumer offsets to zero,
        // so setting this config combined with resetting will cause the application to re-process all the input data in the topic.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    public void mockData() {

    }

    protected void close() {

    }
}
