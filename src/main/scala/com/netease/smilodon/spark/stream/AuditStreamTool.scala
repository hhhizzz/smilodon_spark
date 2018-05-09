package com.netease.smilodon.spark.stream

import com.netease.smilodon.spark.Main
import com.netease.smilodon.spark.factory.KafkaProducerFactory
import com.netease.smilodon.spark.handler.KafkaDStreamSinkExceptionHandler
import com.netease.smilodon.spark.utils.AuditMessageUtil.HDFSAuditMessage
import com.netease.smilodon.spark.utils.{AuditMessageUtil, ConfUtil}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

object AuditStreamTool {
  def saveToKafka(stream: DStream[ClassMessage]): Unit = {
    val config = Main.kafkaParams
    stream
      .map(rdd => AuditMessageUtil.classMessageToHDFSAuditMessage(rdd))
      .map(rdd => AuditMessageUtil.HDFSAuditMessageToJson(rdd))
      .foreachRDD { rdd =>
        rdd.foreachPartition { eachPartition => {
          //setting up the Kafka Producer:
          val kProducer = KafkaProducerFactory.getOrCreateProducer(config)
          val callback = new KafkaDStreamSinkExceptionHandler

          //iterating through each element of RDD to publish each element to Kafka topic:
          eachPartition.toList.foreach { eachElement => {
            val kMessage = new ProducerRecord[String, String]("hdfs_audit_test", null, eachElement)
            callback.throwExceptionIfAny()
            kProducer.send(kMessage)
          }
          }
        }
        }
      }
  }
}
