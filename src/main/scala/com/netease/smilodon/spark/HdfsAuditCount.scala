package com.netease.smilodon.spark

import java.util
import java.util.Calendar

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import scala.util.matching.Regex

object HdfsAuditCount{


  def main(args: Array[String]): Unit = {

    case class RawMessage(message: String, host: String, path: String)
    case class Info(time: Long, allowed: Boolean, ugi: String, ip: String, cmd: String, dst: String, perm: String, proto: String)
    val pattern = new Regex("""([0-9]{4})-?(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(\s+|T)(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9]),([0-9]{3}|[0-9]{2}|[0-9])\s+\[(\d+)\]\s+-\s+(\w+)\s+\[(.*):(.*)@(\w*|\?)\]\s+-\s+(.*)(((\n).*)*)""", "year", "month", "day", "space1", "hour", "minute", "second", "millisecond", "startTime", "level", "threadName", "className", "lineNumber", "message", "extraInfo")
    val infoPattern = new Regex("""allowed=(.*)\s+ugi=(.*)\s+ip=/(.*)\s+cmd=(.*)\s+src=(.*)\s+dst=(.*)\s+perm=(.*)\s+proto=(.*)""", "allowed", "ugi", "ip", "cmd", "src", "dst", "perm", "proto")

    //producer
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hzadg-hadoop-dev10.server.163.org:6667")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val conf = new SparkConf().setMaster("local[2]")
    conf.setAppName("Log_analyse")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.stopSparkContextByDefault", "true")


    val ssc = new StreamingContext(conf, Seconds(10))

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hzadg-hadoop-dev10.server.163.org:6667",
      "group.id" -> "test",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol"->"SASL_PLAINTEXT"
    )
    val raw_message = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("hadoop_audit"), kafkaParams)
    ).map(_.value())

    /*
      type:  String
      like: {"message":....}
    */

    val message = raw_message.map { data =>
      val jsonObject = new JSONObject(data)
      RawMessage(jsonObject.getString("message"), jsonObject.getString("host"), jsonObject.getString("path"))
    }.filter { data =>
      val pathArray = data.path.split("/")
      pathArray.last.equals("hdfs-audit.log")
    }

    val info = message.map { data =>
      val messageResult = pattern.findAllIn(data.message)
      messageResult.next()
      val infoMessage = messageResult.group("message")
      val calendar = Calendar.getInstance()
      calendar.set(messageResult.group("year").toInt, messageResult.group("month").toInt, messageResult.group("day").toInt, messageResult.group("hour").toInt, messageResult.group("minute").toInt, messageResult.group("second").toInt)
      val infoTime = calendar.getTime
      val infoResult = infoPattern.findAllIn(infoMessage)
      infoResult.next()
      Info(infoTime.getTime, infoResult.group("allowed").toBoolean, infoResult.group("ugi"), infoResult.group("ip"), infoResult.group("cmd"), infoResult.group("dst"), infoResult.group("perm"), infoResult.group("proto"))
    }.persist()

    info.foreachRDD { rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._


      /*
        +-------+-------------------+--------------+-----------+----+--------------------+-----+
        |allowed|                ugi|            ip|        cmd| dst|                perm|proto|
        +-------+-------------------+--------------+-----------+----+--------------------+-----+
        |   true|spark (auth:SIMPLE)|10.201.168.152|getfileinfo|null|                null|  rpc|
        |   true|spark (auth:SIMPLE)|10.201.168.152|     create|null|spark:hadoop:rw-r...|  rpc|
      */
      val infoDF = rdd.map {
        case Info(time: Long, allowed: Boolean, ugi: String, ip: String, cmd: String, dst: String, perm: String, proto: String) => (time, allowed, ugi, ip, cmd, dst, perm, proto)
      }.toDF("time", "allowed", "ugi", "ip", "cmd", "dst", "perm", "proto").persist()
      infoDF.createOrReplaceTempView("info")
      infoDF.show()
    }

    //    val successInfo = info.filter(_.allowed)
    //
    //    val cmdTypecount = successInfo
    //      .map(data => (data.cmd,1))
    //      .reduceByKey(_+_)
    //
    //
    //    info.foreachRDD { rdd =>
    //      val count = rdd.count();
    //      println("the action count:" + count);
    //      val message = new ProducerRecord[String, String]("action_count", null, count.toString)
    //      producer.send(message)
    //    }
    //    successInfo.foreachRDD{ rdd =>
    //      println("the successful action count:"+ rdd.count())
    //    }
    //    cmdTypecount.foreachRDD{rdd =>
    //      println("cmd success cound is ")
    //      rdd.foreach(println)
    //      println()
    //    }


    ssc.start()
    ssc.awaitTermination()
  }
}
