// Databricks notebook source exported at Sat, 26 Dec 2015 13:46:28 UTC
// MAGIC %md # Step 1: Understand your Data
// MAGIC 
// MAGIC The first step of doing anything with data is taking a look at it.
// MAGIC  - What's the schema
// MAGIC  - What's the distribution of data
// MAGIC  - Is it dense or sparse
// MAGIC 
// MAGIC This notebook contains some example data analysis techniques before
// MAGIC training a recommendation system. Therefore the dataset used should
// MAGIC have columns regarding a user, an item, and the rating of that user
// MAGIC for that item.

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val df = ... // Load your data

// COMMAND ----------

// MAGIC %md Take a look at schema first

// COMMAND ----------

df.printSchema

// COMMAND ----------

// MAGIC %md Get some summary statistics for numerical columns

// COMMAND ----------

df.describe().show()

// COMMAND ----------

val userColumn = "YOUR_USER_COLUMN" // the name of the column containing user id's in the DataFrame
val itemColumn = "YOUR_ITEM_COLUMN" // the name of the column containing item id's in the DataFrame
val ratingColumn = "YOUR_RATING_COLUMN" // the name of the column containing ratings in the DataFrame

// COMMAND ----------

// MAGIC %md ### Let's look at the average ratings each user gave and the average rating each product received

// COMMAND ----------

val userRatings = df.groupBy(userColumn).agg(
  mean(ratingColumn).as("avgRating"),
  count(ratingColumn).as("numRatings")).sort($"avgRating".desc, $"numRatings".desc)

// COMMAND ----------

userRatings.show()

// COMMAND ----------

val prodRatings = df.groupBy(itemColumn).agg(
  mean(ratingColumn).as("avgRating"),
  count(ratingColumn).as("numRatings")).sort($"avgRating".desc, $"numRatings".desc)

// COMMAND ----------

prodRatings.show()

// COMMAND ----------

// MAGIC %md ### Let's create a histogram to check out the distribution of ratings
// MAGIC 
// MAGIC We will use the Bucketizer available in spark.ml to create the histogram

// COMMAND ----------

import org.apache.spark.ml.feature._

// COMMAND ----------

df.select(min(ratingColumn), max(ratingColumn)).show()

// COMMAND ----------

val minRating = 1 // copy values from above
val maxRating = 5 // copy values from above
val step = 0.5

// COMMAND ----------

// create a range of values from minRating until maxRating by a meaningful step, e.g. 1 to 5 by 0.5
val splits = Array.tabulate(math.ceil(maxRating - minRating).toInt * (1 / step).toInt + 1)(i => i * step + minRating)

// COMMAND ----------

val bucketizer = new Bucketizer()
bucketizer.setSplits(splits)
bucketizer.setInputCol("avgRating")
bucketizer.setOutputCol("bucket")

// COMMAND ----------

// MAGIC %md Look at the distribution of the average ratings of users

// COMMAND ----------

val userRatingBuckets = bucketizer.transform(userRatings).cache()

// COMMAND ----------

userRatingBuckets
  .groupBy("bucket")
  .agg(count("*").as("numUsers"))
  .sort("bucket")
  .select(concat(concat($"bucket" * step + minRating, lit(" - ")), ($"bucket" + 1) * step + minRating).as("rating"), $"numUsers").show()

// COMMAND ----------

// MAGIC %md Look at the distribution of the average ratings of products

// COMMAND ----------

val prodRatingBuckets = bucketizer.transform(prodRatings).cache()

// COMMAND ----------

prodRatingBuckets
  .groupBy("bucket")
  .agg(count("*").as("numProds"))
  .sort("bucket")
  .select(concat(concat($"bucket" * step + minRating, lit(" - ")), ($"bucket" + 1) * step + minRating).as("rating"), $"numProds").show()

// COMMAND ----------

