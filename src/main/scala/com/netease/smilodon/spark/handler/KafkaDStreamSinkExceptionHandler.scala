package com.netease.smilodon.spark.handler

import java.util.concurrent.atomic.AtomicReference

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class KafkaDStreamSinkExceptionHandler extends Callback {

  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
    lastException.set(Option(exception))

  def throwExceptionIfAny(): Unit =
    lastException.getAndSet(None).foreach(ex => throw ex)

}
