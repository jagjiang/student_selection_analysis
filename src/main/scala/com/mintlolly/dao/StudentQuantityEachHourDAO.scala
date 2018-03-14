package com.mintlolly.dao

import com.mintlolly.domain.StudentQuantityEachHour
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * Create by jag on 2018/2/26
  */
object StudentQuantityEachHourDAO {
  val tableName = "number_div_time"
  val columeFimaly="info"
  val qualifer = "student_quantity"

  def save(list:ListBuffer[StudentQuantityEachHour]):Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.chooseTime),
        Bytes.toBytes(columeFimaly),
        Bytes.toBytes(qualifer),
        ele.studentQuantity)
    }
  }
}
