package com.netease.smilodon.spark.factory

import scala.collection.mutable

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducerFactory {

  import scala.collection.JavaConverters._

  private val Log = Logger.getLogger(getClass)

  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[String,String]]()

  def getOrCreateProducer(config: Map[String, Object]): KafkaProducer[String,String] = {

    val defaultConfig = Map(
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    )

    val finalConfig = defaultConfig ++ config

    Producers.getOrElseUpdate(
      finalConfig, {
        Log.info(s"Create Kafka producer , config: $finalConfig")
        val producer = new KafkaProducer[String,String](finalConfig.asJava)

        sys.addShutdownHook {
          Log.info(s"Close Kafka producer, config: $finalConfig")
          producer.close()
        }

        producer
      })
  }
}
