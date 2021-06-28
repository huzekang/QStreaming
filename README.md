 ## 编译项目
 移动到项目根目录。
 执行：
 ```
 mvn clean install -DskipTests  
```

## 本机测试执行SQL作业
### 数据稽查
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*]  \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/hdfsQualityCheck.dsl
```

### 兼容原生spark-sql
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*]  \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/originalSQL.dsl
```

### 批处理

#### 启动hive support
```
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*]  \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/hive2console.dsl -c stream.hive.enable=true
```

#### 写入hudi数据源
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[2] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--jars usql-hudi/target/usql-hudi-1.0.0-jar-with-dependencies.jar \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/hdfs2hudi.dsl
```

#### 读取hudi数据源
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--jars usql-hudi/target/usql-hudi-1.0.0-jar-with-dependencies.jar \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/hudi2console.dsl
```

#### 读取es数据源
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--jars usql-elasticsearch6/target/usql-elasticsearch6-1.0.0-jar-with-dependencies.jar \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/es2console.dsl
```

#### 读取faker数据源
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--jars usql-faker/target/usql-faker-1.0.0.jar \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/faker2console.dsl
```
#### 读取hdfs数据源
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/hdfs2console.dsl
```

### 流处理

#### 读取csv文件
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/csvstream2console.dsl
```

#### 读取json文件
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/jsonstream2console.dsl
```

#### 读取text文件
```shell script
spark-submit \
--class com.chinaunicom.usql.core.USQLEngine \
--master local[*] \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
usql-standalone/target/usql-1.0.0-spark-2.4.7-2.11-jar-with-dependencies.jar \
-j examples/textstream2console.dsl
```