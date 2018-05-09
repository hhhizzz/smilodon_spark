package com.netease.smilodon.spark.utils

import com.netease.smilodon.spark.factory.KafkaProducerFactory
import com.netease.smilodon.spark.handler.KafkaDStreamSinkExceptionHandler
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.dstream.DStream


class KafkaDStreamSink(dstream: DStream[String]) {
  def sendToKafka(config: Map[String, String], topic: String): Unit = {
    dstream.foreachRDD { rdd =>
      rdd.foreachPartition { records =>
        //setting up the Kafka Producer:
        val kProducer = KafkaProducerFactory.getOrCreateProducer(config)
        val callback = new KafkaDStreamSinkExceptionHandler
        records.toList.foreach{eachElement=>{
          val kMessage=new ProducerRecord[String,String]("kafka_topic",null,eachElement)
          callback.throwExceptionIfAny()
          kProducer.send(kMessage)
        }}
      }
    }
  }
}
