// Databricks notebook source exported at Sat, 26 Dec 2015 09:33:06 UTC
// MAGIC %md # **Kafka Sensor Data Producer**
// MAGIC 
// MAGIC Use this to generate data for running Step 3: Monitoring your Service

// COMMAND ----------

// === Configurations for Kafka ===
val kafkaTopic = "sensor"    // command separated list of topics
val kafkaBrokers = "52.25.255.200:9092,52.25.255.200:9093"   // comma separated list of broker:host

// === Configurations of amount of data to produce ===
val numSensors = 5
val numSecondsToSend = 1000


// COMMAND ----------

import java.util.HashMap
import scala.util.Random
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

// Verify that the Kinesis settings have been set
require(!kafkaTopic.contains("YOUR"), "Kafka topic have not been set")
require(!kafkaBrokers.contains("YOUR"), "Kafka brokers have not been set")


// COMMAND ----------

val mu = 10.0
val sigma = 1.0

// COMMAND ----------

val props = new HashMap[String, Object]()
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
  "org.apache.kafka.common.serialization.StringSerializer")
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
  "org.apache.kafka.common.serialization.StringSerializer")

val producer = new KafkaProducer[String, String](props)

// Generate and send the data
for (round <- 1 to numSecondsToSend) {
  for (sensorNum <- 1 to numSensors) {
    val data = (mu + Random.nextGaussian() / sigma).toString
    val message = new ProducerRecord[String, String](kafkaTopic, sensorNum.toString, data)
    producer.send(message)
  }
  Thread.sleep(1000) // Sleep for a second
}



// COMMAND ----------

