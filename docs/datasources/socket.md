### Input

Sytax:

```sql
CREATE STREAM input table table_identifier (col_name: col_type, ...) using kafka (kafkaOptions, "group-id"=<groupId>) [ROW FORMAT rowFormat];
```

Parameters:

 - table_identifier -   table name of input stream.
 - [col_type](https://github.com/qiniu/QStreaming/blob/master/stream-core/src/main/antlr4/com/qiniu/stream/core/parser/Sql.g4#L169) -   struct type  of kafka value, the possible value can be found [here](https://github.com/qiniu/QStreaming/blob/master/stream-core/src/main/antlr4/com/qiniu/stream/core/parser/Sql.g4#L169)
 - [kafkaOptions](https://github.com/qiniu/QStreaming/blob/master/stream-core/src/main/antlr4/com/qiniu/stream/core/parser/Sql.g4#L143)  - are options indicate how to connect to a kafka topic, please refer to [Structed Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) for the  detail of connector configurations
 - [rowFormat](https://github.com/qiniu/QStreaming/blob/master/stream-core/src/main/antlr4/com/qiniu/stream/core/parser/Sql.g4#L161)  - the row format of kafka value, which could be text/json/csv/avro/regex format
 - groupId -  consumer group id that used to monitor the consumer lag

Examples:

```sql
#connect to kafka with JSON format
create stream input table user_behavior(
  user_id LONG,
  item_id LONG,
  category_id LONG,
  behavior STRING
) using kafka(
  kafka.bootstrap.servers="kafka:9092",
  startingOffsets="earliest",
  subscribe="user_behavior",
  "group-id"="user_behavior"
);

#connect to kafka with CSV FORMAT
 create stream input table user_behavior(
  user_id LONG,
  item_id LONG,
  category_id LONG,
  behavior STRING
) using kafka(
  kafka.bootstrap.servers="kafka:9092",
  startingOffsets="earliest",
  subscribe="user_behavior",
  "group-id"="user_behavior"
) ROW FORMAT CSV;

```

### spark-submit

```shell
$SPARK_HOME/bin/spark-submit
--class com.qiniu.stream.core.Streaming \
--master spark://IP:PORT \
--packages com.qiniu:stream-jdbc:0.1.0, ${driverDependencies} \
stream-standalone-0.1.0-jar-with-dependencies.jar \
-j pathToYourPipeline.dsl 
```

where ${driverDependencies} is the database driver dependency, for example if you would like to connect to mysql, your spark-submit should be as follow:

```shell
$SPARK_HOME/bin/spark-submit
--class com.qiniu.stream.core.Streaming \
--master spark://IP:PORT \
--packages com.qiniu:stream-jdbc:0.1.0,mysql:mysql-connector-java:jar:6.0.6 \
stream-standalone-0.1.0-jar-with-dependencies.jar \
-j pathToYourPipeline.dsl 
```

