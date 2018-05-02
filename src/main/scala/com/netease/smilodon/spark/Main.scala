package com.netease.smilodon.spark

import java.util

import com.netease.smilodon.spark.stream.{ClassMessage, HDFSJmxStreamTool, RunningLogStreamTool}
import com.netease.smilodon.spark.utils.ConfUtil
import com.netease.smilodon.spark.utils.ConfUtil.KafkaArgs
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

  /**
    * spark streaming初始化，从kafka读取数据
    *
    * @return ConsumerRecord 组成的流，内容为[topic,value]
    **/
  def initStream(): Map[String, Object] = {
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaArgs.servers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    conf.setAppName("Log_analyse")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.stopSparkContextByDefault", "true")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", kafkaArgs.ElasticsearchHost)
    if (debug) {
      conf.setMaster("local[2]")
    }

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaArgs.servers,
      "group.id" -> kafkaArgs.groupId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_PLAINTEXT"
    )
    kafkaParams
  }

  /**
    * 程序入口
    **/
  def main(args: Array[String]): Unit = {
    val kafkaParams = initStream()
    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](kafkaArgs.topics, kafkaParams)
    )

    val raw_message = kafkaStream.map(recorder => (recorder.topic(), recorder.value()))

    val classMessage = raw_message.map { data =>
      val topic = data._1
      val value = data._2
      val jsonObject = new JSONObject(value)
      ClassMessage(jsonObject.getString("message"), jsonObject.getString("host"), jsonObject.getString("path"), topic = topic)
    }
    classMessage.foreachRDD(rdd => rdd.foreach(println))
    RunningLogStreamTool.saveToElasticsearch(classMessage)


   // HDFSJmxStreamTool.saveToMySQL(classMessage)


    ssc.start()
    ssc.awaitTermination()


  }

}
