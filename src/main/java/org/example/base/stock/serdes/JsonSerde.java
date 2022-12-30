package org.example.base.stock.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.ArrayDeque;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.example.base.stock.util.FixedSizePriorityQueue;

/**
 * This is simple JSON Ser & des wrapper for simple POJO type T.
 * @param <T>
 */
public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private Gson gson;
    private Class<T> tClass;
    private Type type;

    private JsonSerde() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
//        builder.registerTypeAdapter(Instant.class, new InstantSerializer());
//        builder.registerTypeAdapter(Instant.class, new InstantDeserializer());
        builder.registerTypeAdapter(Instant.class, new InstantTypeAdapter().nullSafe());
        builder.registerTypeAdapter(ArrayDeque.class, new DoubleDequeSerializer());
        builder.registerTypeAdapter(ArrayDeque.class, new DoubleDequeDeserializer());

        this.gson = builder.create();
    }
    public JsonSerde(Class<T> tClass) {
        this();
        this.tClass = tClass;
    }

    public JsonSerde(Type type) {
        this();
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (this.type != null) {
            return gson.fromJson(new String(data, StandardCharsets.UTF_8), type);
        }
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), tClass);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        return this.gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    @Override
    public void close() {
        // intentionally left blank
    }

}
