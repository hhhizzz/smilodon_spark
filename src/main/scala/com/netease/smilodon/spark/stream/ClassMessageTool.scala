package com.netease.smilodon.spark.stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.json.JSONObject

object ClassMessageTool {

  def kafkaStreamToClassMessage(stream: InputDStream[ConsumerRecord[String, String]]): DStream[ClassMessage] = {
    val runningLogRawMessage = stream.map(recorder => (recorder.topic(), recorder.value()))

    val classMessages = runningLogRawMessage.map { data =>
      val topic = data._1
      val value = data._2
      val jsonObject = new JSONObject(value)
      ClassMessage(jsonObject.getString("message"), jsonObject.getString("host"), jsonObject.getString("path"), topic = topic)
    }
    classMessages
  }
}
