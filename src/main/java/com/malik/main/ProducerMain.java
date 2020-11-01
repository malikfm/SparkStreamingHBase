package com.malik.main;

import com.malik.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ProducerMain {
    // Don't serialize object to prevent object is not serializable exception
    private static transient Producer<String, String> kafkaProducer;

    public static void main(String[] args) {
        // Separate by coma for multiple bootstrap server
        String bootstrapServer = "localhost:6667";
        String kafkaTopic = "test_topic";

        /*
         * Use file:// if your file is in local filesystem
         * Remove file:// or use hdfs:// if your file is in HDFS
         */
        String filePath = "file:///home/malik/humidity.json";

        // Instantiate spark without config because spark config will be set in running command
        SparkSession sparkSession = SparkSession.builder()
                .appName("ProducerTest")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        kafkaProducer = new KafkaProducer<>(new KafkaConfig().producerConfig(bootstrapServer));

        // Read file then send the value to kafka
        sparkContext
                .textFile(filePath)
                .foreach(stringJSON -> {
                    kafkaProducer.send(new ProducerRecord<>(kafkaTopic, stringJSON));
                });

        kafkaProducer.close();
    }
}
