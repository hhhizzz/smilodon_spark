# smilodon_spark

编译后会生成alternateLocation里面是依赖的jar包

程序提交方式：

```bash
spark-submit  --class com.netease.smilodon.spark.Main \
--master yarn \
--deploy-mode client \
--name smilodon_test \
--files kafka_client_jaas.conf,/etc/security/keytabs/kafka.client.keytab \
--jars alternateLocation/elasticsearch-hadoop-5.6.8.jar,alternateLocation/spark-streaming-kafka-0-10_2.11-2.1.1.jar,alternateLocation/mysql-connector-java-5.1.46.jar,alternateLocation/kafka-clients-0.10.0.1.jar,alternateLocation/kafka_2.11-0.10.0.1.jar,alternateLocation/json-20180130.jar,alternateLocation/spark-sql-kafka-0-10_2.11-2.1.1.jar \
--conf spark.smilodon.kafka.brokers=hzadg-hadoop-dev10.server.163.org:6667 \
--conf spark.smilodon.kafka.log.topics=ambari_log,system_status,hdfs_log,hive_log,spark2_log,zookeeper_log,yarn_log \
--conf spark.smilodon.kafka.audit.topics=hdfs_audit,yarn_audit \
--conf spark.smilodon.kafka.jmx.topics=hdfs_jmx,yarn_jmx \
--conf spark.smilodon.kafka.group.id=test \
--conf spark.smilodon.mysql.server=<mysql_server> \
--conf spark.smilodon.mysql.username=<mysql_username> \
--conf spark.smilodon.mysql.password=<mysql_password> \
--conf spark.smilodon.elasticsearch.host=<es_server> \
--conf spark.smilodon.elasticsearch.port=9200 \
--conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=kafka_client_jaas.conf \
--conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=kafka_client_jaas.conf \
spark_stream_log-1.0-SNAPSHOT.jar
```
