package com.netease.smilodon.spark.stream

import com.netease.smilodon.spark.LogType
import com.netease.smilodon.spark.LogType.LogType
import com.netease.smilodon.spark.stream.ClassMessageTool.kafkaStreamToClassMessage
import com.netease.smilodon.spark.utils.AuditMessageUtil.HDFSAuditMessage
import com.netease.smilodon.spark.utils.KafkaDStreamSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.language.implicitConversions

object StreamFunctions {
  implicit def KafkaStreamFunctions(stream: InputDStream[ConsumerRecord[String, String]]): KafkaStreamFunctions = new KafkaStreamFunctions(stream)

  implicit def ClassMessageStreamFunctions(stream: DStream[ClassMessage]): ClassMessageStreamFunctions = new ClassMessageStreamFunctions(stream)

  implicit def createKafkaDStreamSink(stream: DStream[String]): KafkaDStreamSink = {
    new KafkaDStreamSink(stream)
  }

  class KafkaStreamFunctions(stream: InputDStream[ConsumerRecord[String, String]]) {
    def toClassMessage: DStream[ClassMessage] = {
      kafkaStreamToClassMessage(stream)
    }
  }

  class ClassMessageStreamFunctions(stream: DStream[ClassMessage]) {
    def saveToElasticsearch(logType: LogType): Unit = {
      if (logType == LogType.RUNNING) {
        RunningLogStreamTool.saveToElasticsearch(stream)
      }
    }

    def saveToMySQL(logType: LogType): Unit = {
      if (logType == LogType.JMX) {
        JmxStreamTool.saveToMySQL(stream)
      }
    }
    def saveToKafka(logType: LogType):Unit={
      if(logType==LogType.AUDIT){
        AuditStreamTool.saveToKafka(stream)
      }
    }
  }
}
