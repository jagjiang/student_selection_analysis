package com.mintlolly.spark

import com.mintlolly.dao.{StudentQuantityEachAcademyDAO, StudentQuantityEachCourseDAO, StudentQuantityEachHourDAO, StudentQuantityGradeOfCourseAcademyDAO}
import com.mintlolly.domain._
import com.mintlolly.utils.DataUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.mutable.ListBuffer

/**
  * Create by jag on 2018/2/26
  */
object ProcessStudentDataApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ProcessStudentDataApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(60))

    //从kafka获取数据
    val kafkaParams = Map[String,Object](
          "bootstrap.servers" -> "hadoop:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "use_a_separate_group_id_for_each_stream",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )
    val topics = Array("first_kafka_topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //查看接受到的数据
    stream.map(record =>record.value()).print()
    //数据清洗
    val data = stream.map(record =>record.value())

    val cleanData = data.map(line => {
      val data =line.split("\t")
      CleanData(data(0),data(1),data(2),data(3),DataUtils.parseToMinute(data(4)),data(5),data(6),data(7),Integer.parseInt(data(8)))
    }).filter(cleanData =>cleanData.flag ==1)

    cleanData.print();
    //统计各开设学院选课人数
    cleanData.map(x => {
      (x.courseAcademy,1)
    }).reduceByKey(_+_).foreachRDD( rdd =>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[StudentQuantityEachAcademy]
        partitionRecords.foreach(pair =>{
          list.append(StudentQuantityEachAcademy(pair._1,pair._2))
        })
        StudentQuantityEachAcademyDAO.save(list)
      })
    })

    //一天 24小时每个小时的人数
    cleanData.map(x=>{
      (x.chooseTime.substring(0,10),1)
    }).reduceByKey(_+_).foreachRDD( rdd =>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[StudentQuantityEachHour]
        partitionRecords.foreach(pair =>{
          list.append(StudentQuantityEachHour(pair._1,pair._2))
        })
        StudentQuantityEachHourDAO.save(list)
      })
    })







    //各个课程的人数
    cleanData.map(x => {
      (x.course,1)
    }).reduceByKey(_+_)foreachRDD( rdd =>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[StudentQuantityEachCourse]
        partitionRecords.foreach(pair =>{
          list.append(StudentQuantityEachCourse(pair._1,pair._2))
        })
        StudentQuantityEachCourseDAO.save(list)
      })
    })

    //年级人数分布 rowkey 学生年级+课程学院
    cleanData.map(x => {
      (x.grade+'.'+x.courseAcademy,1)
    }).reduceByKey(_+_)foreachRDD( rdd =>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[StudentQuantityGradeOfCourseAcademy]
        partitionRecords.foreach(pair =>{
          list.append(StudentQuantityGradeOfCourseAcademy(pair._1,pair._2))
        })
        StudentQuantityGradeOfCourseAcademyDAO.save(list)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
