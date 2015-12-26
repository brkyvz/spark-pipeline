# spark-pipeline
Example End-to-End Data Pipeline with Apache Spark from Data Analysis to Data Product

### Scenario 

As an e-commerce company, we would like to recommend products that users 
may like in order to increase sales and profit.

### Notebooks / Source Code

The notebooks are accessible at [http://brkyvz.github.io/spark-pipeline](http://brkyvz.github.io/spark-pipeline).

Right now, only Scala examples are available.

### Procedure

This example pipeline shows steps on how to start with analyzing your data,
training a machine learning model, and then monitoring the health of your
web service using Spark.

 - Step 1: We will first start out by analyzing our data. If you don't have a dataset
readily available, feel free to use the movielens dataset provided with Spark. You may
use the code available under step-0/movielens.scala for this purpose.

 - Step 2: We will train a recommender system using Spark ML's ALS algorithm. We will
use cross-validation to pick the best hyper-parameters and then make predictions on a
test dataset.
  The model then can be exported to a database, such as Cassandra, for interactive
querying through a Web Service.

 - Step 3: We will monitor the response times of our web services using Spark Streaming
and send notifications when we come across an anomalous web service.

### Contributing

Feel free to submit comments, issues using the issues section. Source code contributions for
other languages, (python, java) are welcome!


