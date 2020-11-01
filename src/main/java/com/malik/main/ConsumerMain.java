package com.malik.main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.malik.config.HBaseConfig;
import com.malik.config.KafkaConfig;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerMain {
    public static void main(String[] args) throws InterruptedException {
        // Separate by coma for multiple bootstrap server
        String bootstrapServer = "localhost:6667";
        String groupName = "group_test";
        String autoOffsetReset = "earliest";

        // List of topics, consumer is able to consume from multiple topics
        List<String> kafkaTopics = new ArrayList<>();
        kafkaTopics.add("test_topic");

        // Separate by comma for multiple hbase master and zookeeper quorum
        String hbaseMaster = "localhost";
        String zkQuorum = "localhost:2181";
        String outputTable = "test_table";

        // Instantiate spark without config because spark config will be set in running command
        SparkSession sparkSession = SparkSession.builder()
                .appName("ConsumerTest")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(5000));

        Map<String, Object> consumerConfig = new KafkaConfig()
                .consumerConfig(bootstrapServer, groupName, autoOffsetReset);
        HBaseConfig hBaseConfig = new HBaseConfig(outputTable, hbaseMaster, zkQuorum);

        // Create stream from kafka
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils
                .createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(kafkaTopics, consumerConfig)
                );

        directStream.foreachRDD(javaRDD -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();

            // Data transformation
            JavaPairRDD<ImmutableBytesWritable, Put> pairRDD = javaRDD
                    .flatMap(consumerRecord -> {
                        Map<String, Object> mapValue = convertJSONToMap(consumerRecord.value());
                        List<Map<String, Object>> newValue = new ArrayList<>();

                        // Reform the data
                        mapValue.keySet()
                                .stream()
                                .filter(stringKey -> !stringKey.equals("datetime"))
                                .forEach(stringKey -> {
                                    Map<String, Object> map = new HashMap<>();
                                    map.put("city", stringKey);
                                    map.put("humidity", mapValue.get(stringKey.toLowerCase().replace(" ", "_")));
                                    map.put("datetime", mapValue.get("datetime").toString().replace(" ", "_"));
                                    newValue.add(map);
                                });

                        return newValue.iterator();
                    })
                    .mapToPair(mapValue -> {
                        /*
                         * Set city name as row id and datetime as column qualifier
                         * So a row will represent a city and all its humidity data
                         */
                        Put put = new Put(Bytes.toBytes((String) mapValue.get("city")));
                        put.addColumn(
                                Bytes.toBytes("humidity"),
                                Bytes.toBytes((String) mapValue.get("datetime")),
                                Bytes.toBytes((String) mapValue.get("humidity"))
                        );

                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    });

            pairRDD.saveAsNewAPIHadoopDataset(hBaseConfig.hbaseJob().getConfiguration());

            /*
             * Commit offset manually
             * enable.auto.commit in kafka config must be set to false
             */
            ((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * Convert json object to map
     * @param json - string json object
     * @return key value object from converted json
     * @throws IOException
     */
    private static Map<String, Object> convertJSONToMap(String json) throws IOException {
        return new ObjectMapper().readValue(json, Map.class);
    }
}
