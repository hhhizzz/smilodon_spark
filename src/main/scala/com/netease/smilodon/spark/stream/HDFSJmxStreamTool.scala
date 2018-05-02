package com.netease.smilodon.spark.stream

import com.netease.smilodon.spark.utils.{ConfUtil, JmxMessageUtil}
import com.netease.smilodon.spark.Main
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream

object HDFSJmxStreamTool  {
  def saveToMySQL(dStream: DStream[ClassMessage]): Unit = {
    val kafkaArgs = ConfUtil.getKafkaArgs(Main.debug)
    val datanodeJmxMessage = dStream.filter(data => data.path.contains("hadoop-metrics-datanode"))


    val message = datanodeJmxMessage.map { data =>
      JmxMessageUtil.messageToJmxMessage(data.message)
    }


    val datanode_message = message
      .filter(data => data.key == "dfs.datanode")
      .map(data => JmxMessageUtil.jmxMessageToHDFSJmx(data))
      .persist()

    datanode_message.foreachRDD { rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = rdd.toDF()
      df.write.format("jdbc").mode(SaveMode.Append)
        .option("url", "jdbc:mysql://" + kafkaArgs.databaseHost)
        .option("dbtable", "jmx.jmx_hdfs")
        .option("user", kafkaArgs.databaseUsername)
        .option("password", kafkaArgs.databasePassword)
        .option("driver", "com.mysql.jdbc.Driver")
        .save()
    }
  }
}
