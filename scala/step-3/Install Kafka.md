# Step 3: Monitor Your Services

For Step 3 we need Kafka.

Here are some simple steps to get you started. For more details, please visit [http://docs.confluent.io/1.0.1/quickstart.html](http://docs.confluent.io/1.0.1/quickstart.html)

### Download Confluent's package

```sh
$ wget http://packages.confluent.io/archive/1.0/confluent-1.0.1-2.10.4.zip
$ unzip confluent-1.0.1-2.10.4.zip
$ cd confluent-1.0.1
```

### Start Zookeeper from its own terminal.

```sh
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties
```

### Start Kafka, also in its own terminal.

```sh
$ ./bin/kafka-server-start ./etc/kafka/server.properties
```

### Kafka now should be running on localhost:9092 and ZooKeeper should be on localhost:2181

### Add the topic "sensor" to Kafka

```sh
$ bin/kafka-topics --zookeeper localhost:2181 --create --topic sensor --partitions 2 --replication-factor 1
```

To follow the stream from the console, use:

```sh
$ bin/kafka-console-consumer --zookeeper localhost:2181 --topic sensor
```

To use spark-shell to send data to Kafka, start it with:

`$ bin/spark-shell --packages org.apache.spark:spark-streaming-kafka_2.10:$SPARK_VERSION`

Please don't forget to replace `$SPARK_VERSION`.

Use the Kafka Data Producer Notebook for data generation.

