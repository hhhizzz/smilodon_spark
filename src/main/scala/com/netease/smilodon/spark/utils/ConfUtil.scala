package com.netease.smilodon.spark.utils

import org.apache.spark.SparkConf

/**
  * @author huangqiwei@corp.netease.com
  *         配置项生成类，只能用于jvm环境，executor上运行会抛错
  **/
object ConfUtil {

  case class KafkaArgs(servers: String, logTopics: Array[String], auditTopics: Array[String], JMXTopics: Array[String], groupId: String, databaseHost: String, databaseUsername: String, databasePassword: String, ElasticsearchHost: String, ElasticsearchPort: Integer = 9200)


  private val conf: SparkConf = new SparkConf()
  private val server: String = conf.get("spark.smilodon.kafka.brokers")
  private val logTopics: Array[String] = conf.get("spark.smilodon.kafka.log.topics").split(",")
  private val auditTopics: Array[String] = conf.get("spark.smilodon.kafka.audit.topics").split(",")
  private val JMXTopics: Array[String] = conf.get("spark.smilodon.kafka.jmx.topics").split(",")
  private val groupId: String = conf.get("spark.smilodon.kafka.group.id")
  private val databaseHost: String = conf.get("spark.smilodon.mysql.server")
  private val databaseUsername: String = conf.get("spark.smilodon.mysql.username")
  private val databasePassword: String = conf.get("spark.smilodon.mysql.password")
  private val elasticsearchHost: String = conf.get("spark.smilodon.elasticsearch.host")
  private val elasticsearchPort: Int = conf.get("spark.smilodon.elasticsearch.port").toInt
  private val kafkaArgs: KafkaArgs = KafkaArgs(server, logTopics, auditTopics, JMXTopics, groupId, databaseHost, databaseUsername, databasePassword, elasticsearchHost, elasticsearchPort)


  def getConf: SparkConf = {
    conf
  }

  def getKafkaArgs(debug: Boolean): KafkaArgs = {
    kafkaArgs
  }
}
