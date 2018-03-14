package com.mintlolly.dao

import com.mintlolly.domain.StudentQuantityEachAcademy
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

import scala.collection.mutable.ListBuffer

/**
  * Create by jag on 2018/2/26
  */
object StudentQuantityEachAcademyDAO {
  val tableName = "number_div_academy"
  val columeFimaly="info"
  val qualifer = "student_quantity"

  def save(list:ListBuffer[StudentQuantityEachAcademy]):Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.courseAcademy),
        Bytes.toBytes(columeFimaly),
        Bytes.toBytes(qualifer),
        ele.studentQuantity)
    }
  }
}
