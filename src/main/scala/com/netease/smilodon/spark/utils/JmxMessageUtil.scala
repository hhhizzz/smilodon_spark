package com.netease.smilodon.spark.utils

/**
  * 处理Jmx数据的工具类
  *
  * @author huangqiwei@corp.netease.com
  **/
object JmxMessageUtil {


  /** jmx基本类
    *
    * @param timestamp 时间戳
    * @param key       jmx数据中的类型
    * @param data      jmx中key后面的数据
    * */
  case class JmxMessage(timestamp: Long, key: String, data: String)

  /**
    * hdfs jmx基本类
    *
    * @param hostname 主机名
    *                 剩余为两项基本属性，未来可扩展
    **/
  case class DatanodeJmx(timestamp: Long, hostname: String, flushNanosNumOps: Int, flushNanosAvgTime: Double)

  /**
    * 将message转换为JmxMessage
    *
    * @param message 为字符串，基本结构如下所示
    *                "1523240249992 jvm.JvmMetrics: Context=jvm, ProcessName=DataNode, SessionId=null, Hostname=hzadg-hadoop-dev8.server.163.org, MemNonHeapUsedM=38.531563, MemNonHeapCommittedM=39.25, MemNonHeapMaxM=-9.536743E-7, MemHeapUsedM=169.22484, MemHeapCommittedM=465.5, MemHeapMaxM=910.5, MemMaxM=910.5, GcCountPS Scavenge=2, GcTimeMillisPS Scavenge=42, GcCountPS MarkSweep=1, GcTimeMillisPS MarkSweep=42, GcCount=3, GcTimeMillis=84, GcNumWarnThresholdExceeded=0, GcNumInfoThresholdExceeded=0, GcTotalExtraSleepTime=87, ThreadsNew=0, ThreadsRunnable=18, ThreadsBlocked=0, ThreadsWaiting=5, ThreadsTimedWaiting=22, ThreadsTerminated=0, LogFatal=0, LogError=15, LogWarn=0, LogInfo=91"
    *                处理方式为先使用":"分隔，再用空格分隔取出时间戳和key，剩余的都是data
    **/
  def messageToJmxMessage(message: String): JmxMessage = {
    val message_split = message.split(":")
    val message_timestamp = message_split(0).split(" ")(0).toLong
    val message_key = message_split(0).split(" ")(1)
    val message_data = message_split(1)
    JmxMessage(message_timestamp, message_key, message_data)
  }

  /**
    * 将jmx转换为hdfsjmx
    *
    * @param jmxMessage jmx数据
    **/
  def jmxMessageToHDFSJmx(jmxMessage: JmxMessage): DatanodeJmx = {
    val data = jmxMessage.data.replace(" ", "")
    val datas = data.split(",")
    var flushNanosNumOps = 0
    var flushNanosAvgTime: Double = 0.0
    var hostname = ""
    for (key_value <- datas) {
      val key_values = key_value.split("=")
      val key = key_values(0)
      val value = key_values(1)
      key match {
        case "FlushNanosNumOps" => flushNanosNumOps = value.toInt
        case "FlushNanosAvgTime" => flushNanosAvgTime = value.toDouble
        case "Hostname" => hostname = value
        case _ => ""
      }
    }
    DatanodeJmx(jmxMessage.timestamp, hostname, flushNanosNumOps, flushNanosAvgTime)
  }
}
