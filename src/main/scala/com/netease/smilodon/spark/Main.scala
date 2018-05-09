package com.netease.smilodon.spark

import java.util

import com.netease.smilodon.spark.stream.{ClassMessage, ClassMessageTool, JmxStreamTool, RunningLogStreamTool}
import com.netease.smilodon.spark.utils.ConfUtil
import com.netease.smilodon.spark.utils.ConfUtil.KafkaArgs
import com.netease.smilodon.spark.stream.StreamFunctions._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

/**
  * @author huangqiwei@corp.netease.com
  *         jmx数据处理类，处理kafka中jmx的数据
  **/
object Main {
  val debug: Boolean = false //***注意修改
  val conf: SparkConf = ConfUtil.getConf
  val kafkaArgs: KafkaArgs = ConfUtil.getKafkaArgs(debug = debug)
  val kafkaParams:Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> kafkaArgs.servers,
    "group.id" -> kafkaArgs.groupId,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SASL_PLAINTEXT"
  )

  /**
    * spark streaming初始化，从kafka读取数据
    *
    **/
  def initStream(): Unit = {
    conf.setAppName("log_analyse")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.stopSparkContextByDefault", "true")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", kafkaArgs.ElasticsearchHost)
    if (debug) {
      conf.setMaster("local[2]")
    }

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

  }

  /**
    * 程序入口
    **/
  def main(args: Array[String]): Unit = {

    //处理运行日志的流，10秒一次
    val ssc = new StreamingContext(conf, Seconds(10))
    val classMessageRunningLog = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](kafkaArgs.logTopics, kafkaParams)
    ).toClassMessage

//    classMessageRunningLog.saveToElasticsearch(LogType.RUNNING)


    //处理jmx日志的流，10秒一次
    val classMessageJmx = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](kafkaArgs.JMXTopics, kafkaParams)
    ).toClassMessage
    classMessageJmx.saveToMySQL(LogType.JMX)


    //处理审计日志的流，5秒一次
    val classMessageAudit = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](kafkaArgs.auditTopics, kafkaParams)
    ).toClassMessage
    classMessageAudit.saveToKafka(LogType.AUDIT)



    ssc.start()
    ssc.awaitTermination()
  }

}
