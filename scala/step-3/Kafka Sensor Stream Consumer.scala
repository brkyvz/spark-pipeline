// Databricks notebook source exported at Sat, 26 Dec 2015 10:05:56 UTC
// MAGIC %md # **Kafka - Anomaly Detection **
// MAGIC 
// MAGIC In this notebook, we will receive data from Kafka. The data we receive for the purposes of this demo is request latency data. If some of our
// MAGIC servers are taking longer and longer to respond, we will know about it in real time.
// MAGIC 
// MAGIC The end stream `areAbnormal` will contain information about which servers have taken longer to respond for more than 5 cycles. This stream
// MAGIC can then be used to send alerts (for example through email by using Amazon SES).
// MAGIC 
// MAGIC We will also power a dashboard of the latency data with the `sensorStream` stream.
// MAGIC 
// MAGIC The Kafka stream will have data in the format `(sensorName: String, latency: String)`. Latency is actually a `Double` value, but encoded as `String` for simplicity.

// COMMAND ----------

// MAGIC %md ## Imports
// MAGIC 
// MAGIC Import all the necessary libraries. If you see any error here, you have to make sure that you have attached the necessary libraries to the attached cluster. 
// MAGIC 
// MAGIC In this particular notebook, make sure you have attached Maven dependencies `spark-streaming-kafka-assembly` for same version of Spark as your cluster.

// COMMAND ----------

import java.nio.ByteBuffer
import scala.util.Random

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka._

// COMMAND ----------

// MAGIC %md ## Setup: Define your Kafka configurations
// MAGIC 
// MAGIC Setup your Kafka configurations, i.e. topics to listen to and list of brokers in the format `broker_ip:port`

// COMMAND ----------

val kafkaTopics = "YOUR_KAFKA_TOPICS"    // comma separated list of topics
val kafkaBrokers = "YOUR_KAFKA_BROKERS"   // comma separated list of broker:host

def createKafkaStream(ssc: StreamingContext): DStream[(Int, Double)] = {
  val topicsSet = kafkaTopics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)
  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map { case (key, value) =>
    (key.toInt, value.toDouble)
  }
}

// COMMAND ----------

// Verify that the Kinesis settings have been set
require(!kafkaTopic.contains("YOUR"), "Kafka topic have not been set")
require(!kafkaBrokers.contains("YOUR"), "Kafka brokers have not been set")

// COMMAND ----------

// MAGIC %md ## Setup: Define the function that sets up the StreamingContext
// MAGIC 
// MAGIC This function has to create a new StreamingContext and set it up with all the input stream, transformation and output operations.
// MAGIC 
// MAGIC But it must not start the StreamingContext, it is just to capture all the setup code necessary for your streaming application in one place.

// COMMAND ----------

val batchIntervalSeconds = 1
val checkpointDir = "dbfs:/home/burak/kafka"

// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {
    
  // Create a StreamingContext
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))

  // Get the latency stream from the source
  val sensorStream = createKafkaStream(ssc)
  
  /** 
   * Helper function to calculate the mean latency and standard deviation of the latency for a given sensor.
   * Uses the count, sum, and the sum of squares of all values seen so far to calculate these values.
   */
  def calculateMeanAndStd(count: Long, sum: Double, squaresSum: Double): (Double, Double) = {
    val mean = sum / count
    val variance = squaresSum / (count - 1) - (mean * mean / (count - 1)) * count
    (mean, math.sqrt(variance))
  }
  
  /** 
   * Our key is the sensor name. The value is the latency of the web service. We keep track of the count of
   * measurements we received, the sum of the latencies so far and the sum of the squares of the latencies.
   *
   * We use these to calculate the running mean and standard deviation of the latencies. Then we use the mean
   * and standard deviation to decide whether the current received latency fits a normal distribution with
   * a confidence interval of 95%. If it does, we return 0 for that sensor. If it doesn't we return a 1.
   *
   * Notice that our mean and variance will continue updating, which means that if we don't take action with
   * the alerts, then the mean and variance will adapt to the anomalous behavior.
   */
  def mappingFunction(key: Int, value: Option[Double], state: State[(Long, Double, Double)]): (Int, Int) = {
    if (state.exists()) {
      // get the existing state for the sensor
      val (count, sum, sumSquares) = state.get()
      if (count > 10) {
        value.map { v =>
          val (mean, std) = calculateMeanAndStd(count, sum, sumSquares)
          state.update((count + 1, sum + v, sumSquares + (v * v)))
          val lb = mean - 2 * std
          val ub = mean + 2 * std
          // decide whether value is outside our 95% confidence range
          val isAbnormal = if (lb >= v || ub <= v) 1 else 0
          (key, isAbnormal)
        }.getOrElse((key, 0))
      } else {
        // if we don't have enough measurements (> 10), we just update the state and return a 0.
        value.foreach { v =>
          state.update((count + 1, sum + v, sumSquares + (v * v)))
        }
        (key, 0)
      }
    } else {
      // The first time we received a measurement for this sensor. Update the state
      // and return a 0.
      value.map(v => state.update((1L, v, v * v)))
      (key, 0)
    }
  }
  
  // Define the state spec using our mapping function
  val spec = StateSpec.function(mappingFunction _).numPartitions(2)
  
  // This stream is not the state. Rather it is whether we deemed the latency value anomalous or not.
  // The state can be accessed through `sensorAbnormalities.stateSnapshots()`, but we don't need it for this example.
  val sensorAbnormalities = sensorStream.mapWithState[(Long, Double, Double), (Int, Int)](spec)
  
  // Decide whether we received more than 5 abnormal values in a sliding window of 10 seconds.
  val areAbnormal = sensorAbnormalities.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10)).filter(_._2 > 5)
  
  // Print the abnormal sensors. Here we can trigger alerts, send emails, etc... using foreachRDD, and rdd.foreachPartition{...}.
  areAbnormal.print()
  
  // Register the sensor data as a table for a window of 30 seconds, so that we can power a dashboard
  sensorStream.transform { (rdd, time) =>
    // Normally, the timestamp of the latency will be with the data itself, but for the purposes of the demo, we add them here
    rdd.map(v => (v._1, v._2, time.milliseconds / 1000))
  }.window(Seconds(30)).foreachRDD { (rdd, time) =>
    // Turn the timestamp to a human readable time representation.
    rdd.toDF("sensor", "latency", "timestamp").withColumn("time", from_unixtime($"timestamp", "HH:mm:ss")).registerTempTable("sensor_data")
  }
  
  // To make sure data is not deleted by the time we query it interactively
  ssc.remember(Minutes(1))
  
  ssc.checkpoint(checkpointDir)
  
  println("Creating function called to create new StreamingContext")
  ssc
}

// COMMAND ----------

// MAGIC %md ## Start/Restart: Stop existing StreamingContext if any and start/restart the new one

// COMMAND ----------

// Stop any existing StreamingContext 
val stopActiveContext = true
if (stopActiveContext) {	
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
} 

// Get or create a streaming context.
val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

// This starts the streaming context in the background. 
ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)

// COMMAND ----------

// MAGIC %md ## Interactive Querying
// MAGIC 
// MAGIC Now let's try querying the table. You can run this command again and again, you will find the numbers changing while data is being sent.

// COMMAND ----------

// MAGIC %sql select * from sensor_data

// COMMAND ----------

// MAGIC %md ### Finally, if you want stop the StreamingContext, you can uncomment and execute the following

// COMMAND ----------

// StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

