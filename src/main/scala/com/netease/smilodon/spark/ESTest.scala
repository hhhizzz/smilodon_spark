package com.netease.smilodon.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

object ESTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("estest").setMaster("local[2]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "hzadg-hadoop-dev7.server.163.org")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val log1 = Map("time" -> "2018-04-16 16:45:51,983+0800", "start_time" -> 6638654, "level" -> "ERROR",
      "thread_name" -> "DataXceiver for client /10.201.168.150:34514 [Waiting for operation #1]",
      "class_name" -> "DataXceiver", "line_number" -> 280,
      "info"-> "hzadg-hadoop-dev6.server.163.org:50010:DataXceiver error processing unknown operation  src: /10.201.168.150:34514 dst: /10.201.168.150:50010",
      "extra_information" -> "java.io.EOFException\n\tat java.io.DataInputStream.readShort(DataInputStream.java:315)\n\tat org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.readOp(Receiver.java:58)\n\tat org.apache.hadoop.hdfs.server.datanode.DataXceiver.run(DataXceiver.java:229)\n\tat java.lang.Thread.run(Thread.java:748)")

    val rdd = sc.makeRDD(Seq(log1))
    val microbatches = mutable.Queue(rdd)
    ssc.queueStream(microbatches).saveToEs("log_test/running_log")
    ssc.start()
    ssc.awaitTermination()
  }
}
