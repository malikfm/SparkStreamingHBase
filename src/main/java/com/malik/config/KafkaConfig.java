package com.malik.config;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KafkaConfig implements Serializable {
    /**
     * Set kafka producer config
     * @param bootstrapServer - kafka bootstrap server/broker
     * @return key value object of config
     */
    public Map<String, Object> producerConfig(String bootstrapServer) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", bootstrapServer);
        configMap.put("request.timeout.ms", 120000);
        configMap.put("key.serializer", StringSerializer.class);
        configMap.put("value.serializer", StringSerializer.class);
        configMap.put("retries", 3);
        configMap.put("compression.type", "snappy");
        configMap.put("batch.size", 24576);
        configMap.put("delivery.timeout.ms", 180000);
        configMap.put("linger.ms", 30000);
        configMap.put("acks", "all");

        return configMap;
    }

    /**
     * Set kafka consumer config
     * @param bootstrapServer - kafka bootstrap server/broker
     * @param groupName - kafka consumer group
     * @param autoOffsetReset - auto offset reset policy
     * @return key value object of config
     */
    public Map<String, Object> consumerConfig(String bootstrapServer, String groupName, String autoOffsetReset) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", bootstrapServer);
        configMap.put("request.timeout.ms", 120000);
        configMap.put("key.deserializer", StringDeserializer.class);
        configMap.put("value.deserializer", StringDeserializer.class);
        configMap.put("group.id", groupName);
        configMap.put("auto.offset.reset", autoOffsetReset);
        configMap.put("enable.auto.commit", false);
        configMap.put("max.poll.records", 250);
        configMap.put("max.poll.interval.ms", 450000);
        configMap.put("session.timeout.ms", 30000);

        return configMap;
    }
}
