package com.netease.smilodon.spark.utils

import java.util.Calendar

import com.netease.smilodon.spark.stream.ClassMessage
import org.json.JSONObject

import scala.util.matching.Regex


/**
  * 处理审计日志的工具类
  *
  * @author huangqiwei@corp.netease.com
  **/
object AuditMessageUtil {

  case class HDFSAuditMessage(time: Long, allowed: Boolean, ugi: String, ip: String, cmd: String, dst: String, perm: String, proto: String)

  def classMessageToHDFSAuditMessage(classMessage: ClassMessage): HDFSAuditMessage = {
    val pattern = new Regex("""([0-9]{4})-?(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(\s+|T)(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9]),([0-9]{3}|[0-9]{2}|[0-9])\s+\[(\d+)\]\s+-\s+(\w+)\s+\[(.*):(.*)@(\w*|\?)\]\s+-\s+(.*)(((\n).*)*)""", "year", "month", "day", "space1", "hour", "minute", "second", "millisecond", "startTime", "level", "threadName", "className", "lineNumber", "message", "extraInfo")
    val infoPattern = new Regex("""allowed=(.*)\s+ugi=(.*)\s+ip=/(.*)\s+cmd=(.*)\s+src=(.*)\s+dst=(.*)\s+perm=(.*)\s+proto=(.*)""", "allowed", "ugi", "ip", "cmd", "src", "dst", "perm", "proto")
    val messageResult = pattern.findAllIn(classMessage.message)
    messageResult.next()
    val infoMessage = messageResult.group("message")
    val calendar = Calendar.getInstance()
    calendar.set(messageResult.group("year").toInt, messageResult.group("month").toInt, messageResult.group("day").toInt, messageResult.group("hour").toInt, messageResult.group("minute").toInt, messageResult.group("second").toInt)
    val infoTime = calendar.getTime
    val infoResult = infoPattern.findAllIn(infoMessage)
    infoResult.next()
    HDFSAuditMessage(infoTime.getTime, infoResult.group("allowed").toBoolean, infoResult.group("ugi"), infoResult.group("ip"), infoResult.group("cmd"), infoResult.group("dst"), infoResult.group("perm"), infoResult.group("proto"))
  }

  def HDFSAuditMessageToJson(hDFSAuditMessage: HDFSAuditMessage):String={
    val jsonObject=new JSONObject()
    jsonObject.put("time",hDFSAuditMessage.time)
    jsonObject.put("allowed",hDFSAuditMessage.allowed)
    jsonObject.put("ugi",hDFSAuditMessage.ugi)
    jsonObject.put("ip",hDFSAuditMessage.ip)
    jsonObject.put("cmd",hDFSAuditMessage.cmd)
    jsonObject.put("dst",hDFSAuditMessage.dst)
    jsonObject.put("perm",hDFSAuditMessage.perm)
    jsonObject.put("proto",hDFSAuditMessage.proto)
    jsonObject.toString
  }
}
