package com.malik.main;

import com.malik.config.HBaseConfig;
import com.malik.config.KafkaConfig;
import com.malik.util.ApplicationUtil;
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
    public static void main(String[] args) throws InterruptedException, IOException {

        // Instantiate Spark
        SparkSession sparkSession = SparkSession.builder()
                .appName("ConsumerMain")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(5000));

        // Instantiate config / util
        ApplicationUtil applicationUtil = new ApplicationUtil();

        Map<String, Object> mapConfig = applicationUtil.convertJSONToMap(args[0]);
        Map<String, Object> mapKafkaConfig = (Map<String, Object>) mapConfig.get("kafka");
        Map<String, Object> mapHbaseConfig = (Map<String, Object>) mapConfig.get("hbase");

        KafkaConfig kafkaConfig = new KafkaConfig((String) mapKafkaConfig.get("broker"), (String) mapKafkaConfig.get("group_name"), (String) mapKafkaConfig.get("auto_offset_reset"), (Boolean) mapKafkaConfig.get("auto_commit"), (Map<String, Integer>) mapKafkaConfig.get("kafka_topics"));
        HBaseConfig hBaseConfig = new HBaseConfig((String) mapHbaseConfig.get("output_table"), (String) mapHbaseConfig.get("master"), (String) mapHbaseConfig.get("quorum"));

        // Create direct stream from Kafka
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Assign(kafkaConfig.kafkaTopics(), kafkaConfig.kafkaConfiguration()));

        directStream.foreachRDD(javaRDD -> {

            // Get offset range
            OffsetRange[] offsetRanges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();

            // Spark transformation
            JavaPairRDD<ImmutableBytesWritable, Put> pairRDD = javaRDD.flatMap(consumerRecord -> {
                Map<String, Object> mapValue = applicationUtil.convertJSONToMap(consumerRecord.value());
                List<Map<String, Object>> newValue = new ArrayList<>();

                mapValue.keySet()
                        .stream()
                        .filter(stringKey -> !stringKey.equals("datetime"))
                        .forEach(stringKey -> {
                            Map<String, Object> map = new HashMap<>();
                            map.put("city", stringKey);
                            map.put("humidity", mapValue.get(stringKey));
                            map.put("datetime", mapValue.get("datetime"));

                            newValue.add(map);
                        });

                return newValue.iterator();
            }).mapToPair(mapValue -> {

                // Set HBase row id. Use city name as row id
                Put put = new Put(Bytes.toBytes(((String) mapValue.get("city")).toLowerCase().replace(" ", "_")));

                // Set HBase column family, column qualifier, and column value. Humidity as column family, datetime value as column qualifier, and humidity value as column value.
                put.addColumn(Bytes.toBytes("humidity"), Bytes.toBytes(((String) mapValue.get("datetime")).replace(" ", "_")), Bytes.toBytes((String) mapValue.get("humidity")));

                return new Tuple2<>(new ImmutableBytesWritable(), put);
            });

            // Spark action
            pairRDD.saveAsNewAPIHadoopDataset(hBaseConfig.hbaseJob().getConfiguration());

            // Commit offset manually
            ((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
