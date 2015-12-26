// Databricks notebook source exported at Sat, 26 Dec 2015 14:00:02 UTC
// MAGIC %md # Step 0: Import your data
// MAGIC 
// MAGIC Use the movielens dataset provided with Spark.

// COMMAND ----------

// assumes you are under $SPARK_HOME
val rdd = sc.textFile("data/mllib/sample_movielens_data.txt").map { line =>
  val Array(user, item, rating) = line.split("::")
  (user, item, rating)
}

// COMMAND ----------

val df = sqlContext.createDataFrame(rdd).toDF("user", "item", "rating")

// COMMAND ----------

df.show

// COMMAND ----------

