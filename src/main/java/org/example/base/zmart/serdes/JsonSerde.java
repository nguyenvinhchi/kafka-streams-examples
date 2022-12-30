package org.example.base.zmart.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * This is simple JSON Ser & des wrapper for simple POJO type T.
 * @param <T>
 */
public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private Gson gson;
    private Class<T> tClass;

    public JsonSerde(Class<T> tClass) {
        this.gson = new GsonBuilder().create();
        this.tClass = tClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
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
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
