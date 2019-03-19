package com.malik.main;

import com.malik.config.KafkaConfig;
import com.malik.util.ApplicationUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Map;

public class ProducerMain {
    private static transient Producer<String, String> kafkaProducer;

    public static void main(String[] args) throws IOException {
        // Instantiate Spark
        SparkSession sparkSession = SparkSession.builder()
                .appName("ProducerMain")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // Instantiate config / util
        ApplicationUtil applicationUtil = new ApplicationUtil();

        Map<String, Object> mapConfig = applicationUtil.convertJSONToMap(args[0]);
        Map<String, Object> mapKafkaConfig = (Map<String, Object>) mapConfig.get("kafka");

        KafkaConfig kafkaConfig = new KafkaConfig((String) mapKafkaConfig.get("broker"));

        // Instantiate Kafka Producer
        kafkaProducer = new KafkaProducer<>(kafkaConfig.kafkaConfiguration());

        // Read file then send its value to Kafka. In my code I use spark to read file.
        sparkContext.textFile("file://" + args[1])
                .foreach(stringJSON -> {

                    // Send message (string JSON) to Kafka
                    kafkaProducer.send(new ProducerRecord<>((String) mapKafkaConfig.get("kafka_topic"), stringJSON));

                });

        // Close Kafka Producer
        kafkaProducer.close();
    }
}
