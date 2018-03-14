package com.mintlolly.spark


import com.mintlolly.dao.StudentQuantityEachHourDAO
import com.mintlolly.domain.StudentQuantityEachHour

import scala.collection.mutable.ListBuffer

/**
  * Create by jag on 2018/3/1
  */
object AddData {
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[StudentQuantityEachHour]
    list.append(StudentQuantityEachHour("2018030101",10))
    list.append(StudentQuantityEachHour("2018030102",20))
    list.append(StudentQuantityEachHour("2018030103",10))
    list.append(StudentQuantityEachHour("2018030104",30))
    list.append(StudentQuantityEachHour("2018030105",40))
    list.append(StudentQuantityEachHour("2018030106",50))
    list.append(StudentQuantityEachHour("2018030107",60))
    list.append(StudentQuantityEachHour("2018030108",70))
    list.append(StudentQuantityEachHour("2018030109",80))
    list.append(StudentQuantityEachHour("2018030110",90))
    list.append(StudentQuantityEachHour("2018030111",110))
    list.append(StudentQuantityEachHour("2018030112",120))
    list.append(StudentQuantityEachHour("2018030113",130))
    list.append(StudentQuantityEachHour("2018030114",140))
    list.append(StudentQuantityEachHour("2018030116",150))
    list.append(StudentQuantityEachHour("2018030115",410))
    list.append(StudentQuantityEachHour("2018030117",160))
    list.append(StudentQuantityEachHour("2018030118",510))
    list.append(StudentQuantityEachHour("2018030119",810))
    list.append(StudentQuantityEachHour("2018030120",160))
    list.append(StudentQuantityEachHour("2018030121",610))
    list.append(StudentQuantityEachHour("2018030122",140))
    list.append(StudentQuantityEachHour("2018030124",160))
    StudentQuantityEachHourDAO.save(list)
  }
}
