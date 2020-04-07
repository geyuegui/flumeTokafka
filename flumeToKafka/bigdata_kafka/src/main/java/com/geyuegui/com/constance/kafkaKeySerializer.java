package com.geyuegui.com.constance;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class kafkaKeySerializer implements Serializer {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
