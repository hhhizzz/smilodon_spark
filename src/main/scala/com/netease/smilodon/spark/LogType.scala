package com.netease.smilodon.spark

object LogType extends Enumeration {
  type LogType = Value
  val RUNNING, AUDIT, JMX = Value
}
