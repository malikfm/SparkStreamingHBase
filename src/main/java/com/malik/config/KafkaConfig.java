package com.malik.config;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConfig implements Serializable {

    private String bootstrapServer;
    private String kafkaGroupName;
    private String autoOffsetReset;
    private Boolean autoCommit;
    private Map<String, Integer> kafkaTopics;

    /**
     * Set variable in constructor. For Kafka Producer.
     * @param bootstrapServer -> String kafka broker (host:port)
     */
    public KafkaConfig(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
        kafkaGroupName = "group1";
        autoOffsetReset = "earliest";
        autoCommit = false;
        kafkaTopics = new HashMap<>();
    }

    /**
     * Set variable in constructor. For Kafka Consumer.
     * @param bootstrapServer -> String kafka broker (host:port)
     * @param kafkaGroupName -> String kafka group
     * @param autoOffsetReset -> String kafka auto offset reset
     * @param autoCommit -> Boolean kafka auto commit
     * @param kafkaTopics -> Map kafka topics (String topic name, Integer partitions)
     */
    public KafkaConfig(String bootstrapServer, String kafkaGroupName, String autoOffsetReset, Boolean autoCommit, Map<String, Integer> kafkaTopics) {
        this.bootstrapServer = bootstrapServer;
        this.kafkaGroupName = kafkaGroupName;
        this.autoOffsetReset = autoOffsetReset;
        this.autoCommit = autoCommit;
        this.kafkaTopics = kafkaTopics;
    }

    /**
     * Set kafka config
     * @return Map kafka configuration
     */
    public Map<String, Object> kafkaConfiguration() {
        Map<String, Object> map = new HashMap<>();
        map.put("bootstrap.servers", bootstrapServer);
        map.put("request.timeout.ms", 120000);

        // consumer
        map.put("key.deserializer", StringDeserializer.class);
        map.put("value.deserializer", StringDeserializer.class);
        map.put("group.id", kafkaGroupName);
        map.put("auto.offset.reset", autoOffsetReset);
        map.put("enable.auto.commit", autoCommit);
        map.put("max.poll.records", 250);
        map.put("max.poll.interval.ms", 450000);
        map.put("session.timeout.ms", 30000);

        // producer
        map.put("key.serializer", StringSerializer.class);
        map.put("value.serializer", StringSerializer.class);
        map.put("retries", 3);
        map.put("compression.type", "snappy");
        map.put("batch.size", 24576);
        map.put("delivery.timeout.ms", 180000);
        map.put("linger.ms", 30000);
        map.put("acks", "all");

        return map;
    }

    /**
     * Set kafka topics and its partitions. It's possible consume multiple topics
     * @return List of TopicPartition
     */
    public List<TopicPartition> kafkaTopics() {
        List<TopicPartition> topicPartitionList = new ArrayList<>();

        kafkaTopics.forEach((topicName, topicPartitions) -> {
            for (int partitionNumber = 0; partitionNumber < topicPartitions; partitionNumber++)
                topicPartitionList.add(new TopicPartition(topicName, partitionNumber));
        });

        return topicPartitionList;
    }
}
