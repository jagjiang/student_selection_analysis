package com.mintlolly.dao

import com.mintlolly.domain.StudentQuantityEachCourse
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * Create by jag on 2018/2/27
  */
object StudentQuantityEachCourseDAO {
  val tableName = "number_div_course"
  val columeFimaly="info"
  val qualifer = "student_quantity"

  def save(list:ListBuffer[StudentQuantityEachCourse]):Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.crouseName),
        Bytes.toBytes(columeFimaly),
        Bytes.toBytes(qualifer),
        ele.studentQuantity)
    }
  }
}
