package com.netease.smilodon.spark.stream

import com.netease.smilodon.spark.{Main, ShowClasses}
import com.netease.smilodon.spark.utils.{ConfUtil, RunningMessageUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.streaming.EsSparkStreaming

object RunningLogStreamTool {
  def saveToElasticsearch(dStream: DStream[ClassMessage]): Unit = {
    val kafkaArgs = ConfUtil.getKafkaArgs(Main.debug)
    //conf.set("es.port", kafkaArgs.ElasticsearchPort.toString)

    val filterMessage = dStream.filter(data => data.topic.contains("_log")).persist()


    val ESStream = filterMessage
      .map(data => RunningMessageUtil.classMessageToRunningMessage(data))
      .map(data => RunningMessageUtil.RunningMessageToElasticsearchMap(data))
      .persist()

    EsSparkStreaming.saveToEs(ESStream,"log_test/running_log")
  }
}
