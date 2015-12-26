// Databricks notebook source exported at Sat, 26 Dec 2015 14:42:30 UTC
// MAGIC %md # Step 2: Build your Service
// MAGIC 
// MAGIC We will build a recommendation engine using Spark MLlib using Alternating Least Squares (ALS).
// MAGIC 
// MAGIC This notebook contains an example workflow of:
// MAGIC  - Pre-processing your data using DataFrames and SQL functions
// MAGIC  - Hyper-Parameter tuning using ML Pipelines: CrossValidator
// MAGIC  - Evaluating your model using RegressionEvaluator

// COMMAND ----------

val df = // Load your DataFrame

// COMMAND ----------

val userColumn = "YOUR_USER_COLUMN" // the name of the column containing user id's in the DataFrame
val itemColumn = "YOUR_ITEM_COLUMN" // the name of the column containing item id's in the DataFrame
val ratingColumn = "YOUR_RATING_COLUMN" // the name of the column containing ratings in the DataFrame

val userIdColumn = userColumn + "Id" // change this to your convenience
val itemIdColumn = itemColumn + "Id" // change this to your convenience

// COMMAND ----------

// MAGIC %md ### Pre-processing
// MAGIC 
// MAGIC In order to use ALS, we need `Integer` or `Long` type id's for users and items. If your dataset meets these requirements,
// MAGIC feel free to skip to the next steps.

// COMMAND ----------

// MAGIC %md Naive Approach: Hash the `String` id's to `Integer`s.

// COMMAND ----------

val hash = udf((value: Any) => value.##)

// COMMAND ----------

val inputDf = df.select(hash(df(userColumn)).as(userIdColumn), hash(df(itemColumn)).as(itemIdColumn), df(ratingColumn)).cache()

// COMMAND ----------

val distinctUsers = df.select(userColumn).distinct().count
val distinctUserIds = inputDf.select(userIdColumn).distinct().count

// COMMAND ----------

distinctUsers - distinctUserIds

// COMMAND ----------

// MAGIC %md If the result of the previous command is 0, continue to the next part, if not, try the following

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// Get distinct users and items, and give them each a unique Id
val users = df.select(userColumn).distinct()
val userIds = users.withColumn(userIdColumn, monotonicallyIncreasingId())
val prods = df.select(itemColumn).distinct()
val prodIds = prods.withColumn(itemIdColumn, monotonicallyIncreasingId())

// COMMAND ----------

val inputDf = df.join(userIds, Seq(userColumn)).join(prodIds, Seq(itemColumn))

// COMMAND ----------

val distinctUsers = inputDf.select(userColumn).distinct().count
val distinctUserIds = inputDf.select(userIdColumn).distinct().count

// COMMAND ----------

// this should now be 0
distinctUserIds - distinctUsers

// COMMAND ----------

// MAGIC %md ### Training the model
// MAGIC 
// MAGIC We will pick hyper-parameters using the CrossValidator

// COMMAND ----------

import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._

import org.apache.spark.sql.types._

// COMMAND ----------

val predictionColumn = "prediction" // change this to your liking
val seed = 25L // for reproducability

// COMMAND ----------

val als = new ALS()
als.setUserCol(userIdColumn)
als.setItemCol(itemIdColumn)
als.setRatingCol(ratingColumn)
als.setPredictionCol(predictionColumn)
als.setSeed(seed)
als.setMaxIter(10) // we could tune this as well, but it's not required

// COMMAND ----------

// Be careful when building your grid. The process may become more and more expensive
val paramGrid = new ParamGridBuilder()
  .addGrid(als.rank, Array(10, 20, 40))
  .addGrid(als.regParam, Array(0.1, 1.0, 10.0))
  .build()

// COMMAND ----------

// we will use Root-Mean-Square-Error (RMSE) to evaluate our error
val rmseEval = new RegressionEvaluator
rmseEval.setLabelCol(ratingColumn)
rmseEval.setPredictionCol(predictionColumn)

// COMMAND ----------

val cv = new CrossValidator()
  .setEstimator(als) // you can also provide a pipeline here instead of just ALS
  .setEvaluator(rmseEval)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(2) // Use 3+ in practice

// COMMAND ----------

// MAGIC %md Split the dataset into training and test. The CrossValidator will further split the training dataset
// MAGIC into cross-validation datasets.

// COMMAND ----------

val Array(trainDf, testDf) = inputDf.select(
  inputDf(userIdColumn).cast(IntegerType), 
  inputDf(itemIdColumn).cast(IntegerType),
  inputDf(ratingColumn)).randomSplit(Array(0.6, 0.4), seed)

// COMMAND ----------

trainDf.cache()
trainDf.count // materialize trainDf

// COMMAND ----------

val cvModel = cv.fit(trainDf)

// COMMAND ----------

// The CrossValidator will use the best performing model against the validation set
val output = cvModel.transform(testDf)

// COMMAND ----------

rmseEval.evaluate(output)

// COMMAND ----------

// MAGIC %md Uh-oh! We may have some users and items we haven't trained for in the test dataset! That will cause `null`'s in predictions
// MAGIC and the evaluator will return NaN

// COMMAND ----------

val nonNull = output.na.drop()

// COMMAND ----------

rmseEval.evaluate(nonNull)

// COMMAND ----------

display(nonNull)

// COMMAND ----------

// MAGIC %md ### From here, how you progress is up to you.
// MAGIC 
// MAGIC Some possible workflows:
// MAGIC  - Save the model to an external data store, e.g. Cassandra
// MAGIC  - Perform Clustering to find similar users, items
// MAGIC  - Find top-k recommendations for users and also store to external data store