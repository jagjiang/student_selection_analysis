package com.mintlolly.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Create by jag on 2018/2/27
  */
object DataUtils {
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")


  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time :String) = {
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {

    println(parseToMinute("2018-2-22 14:46:01"))

  }
}
