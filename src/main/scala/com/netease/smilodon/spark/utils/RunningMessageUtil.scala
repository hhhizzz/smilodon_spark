package com.netease.smilodon.spark.utils


import com.netease.smilodon.spark.stream.ClassMessage

import scala.util.matching.Regex

object RunningMessageUtil {

  case class RunningMessage(host: String, path: String, date: String, startTime: Long, level: String, threadName: String, className: String, lineNumber: String, information: String, extraInformation: String)


  def classMessageToRunningMessage(classMessage: ClassMessage): RunningMessage = {
    val message = classMessage.message
    val pattern = new Regex("""([0-9]{4})-?(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(\s+|T)(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9]),([0-9]{3}|[0-9]{2}|[0-9])\s+\[(\d+)\]\s+-\s+(\w+)\s+\[(.*):(.*)@(\w*|\?)\]\s+-\s+(.*)(((\n).*)*)""", "year", "month", "day", "space1", "hour", "minute", "second", "millisecond", "startTime", "level", "threadName", "className", "lineNumber", "info", "extraInfo")
    val messageMatch = pattern.findAllIn(message)
    messageMatch.next()
    val messageTime = messageMatch.group("year") + "-" + messageMatch.group("month") + "-" + messageMatch.group("day") + " " + messageMatch.group("hour") + ":" + messageMatch.group("minute") + ":" + messageMatch.group("second") + "," + messageMatch.group("millisecond")
    val startTime = messageMatch.group("startTime").toLong
    val level = messageMatch.group("level")
    val threadName = messageMatch.group("threadName")
    val className = messageMatch.group("className")
    val lineNumber = messageMatch.group("lineNumber")
    val info = messageMatch.group("info")
    val extraInfo = messageMatch.group("extraInfo")
    RunningMessage(classMessage.host, classMessage.path, messageTime, startTime, level, threadName, className, lineNumber, info, extraInfo)
  }

  def RunningMessageToElasticsearchMap(runningMessage: RunningMessage): Map[String, Any] = {
    val time = runningMessage.date+"+0800"
    Map(
      "host" -> runningMessage.host,
      "path" -> runningMessage.path,
      "time" -> time,
      "start_time" -> runningMessage.startTime,
      "level" -> runningMessage.level,
      "thread_name" -> runningMessage.threadName,
      "class_name" -> runningMessage.className,
      "line_number" -> runningMessage.lineNumber,
      "information" -> runningMessage.information,
      "extra_information" -> runningMessage.extraInformation)
  }
}
