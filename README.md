# SparkStreamingHBase
This project illustrates how to create Kafka Producer, Kafka Consumer, and insert streaming data into HBase using Java and Spark. Humidity data were taken from https://www.kaggle.com/selfishgene/historical-hourly-weather-data?select=humidity.csv

## Prerequisites
Please ensure that you have met the following requirements:
* Java 8
* Maven
* Apache Spark 2.x
* Apache Kafka 0.10.x
* Apache HBase 1.x

## Using this project
This project consists of two main class:
* ProducerMain: read hourly time series of humidity data (humidity.json) then send to Kafka.
* ConsumerMain: consume data from Kafka, transform the data then save to HBase.

### Build
<pre>mvn install</pre>

### Run
Producer
<pre>spark-submit --class com.malik.main.ProducerMain --master local[2] malik/engine/SparkStreamingHBase-1.0-SNAPSHOT-jar-with-dependencies.jar</pre>

Consumer
<pre>spark-submit --class com.malik.main.ConsumerMain --master local[2] malik/engine/SparkStreamingHBase-1.0-SNAPSHOT-jar-with-dependencies.jar</pre>

Visit `localhost:port` using your browser to monitor your spark job. Default port is 4040.
