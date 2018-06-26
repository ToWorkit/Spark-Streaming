package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.dao.CourseClickCountDAO
import project.domain.{ClickLog, CourseClickCount}
import project.utils.DateUtils

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))

    // 指定Topic信息：从streamingtopic的topic中，每次接受一条消息
    val topic = Map("streamingtopic" -> 1)

    // 创建Kafka输入流，基于Receiver方式，链接到ZK
    val messages = KafkaUtils.createStream(ssc,"192.168.187.116:2181","test",topic)

    // 打印数据
     // messages.map(_._2).print()

    // 数据清洗
    // 124.63.98.156	2018-06-24 12:11:01	"GET /class/112.html HTTP/1.1"	500	https://www.sogou.com/web?query=Spark SQL实战
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      // 课程id
      var courseId = 0
      // 清洗 必须是以 /class 开头的数据，其他数据忽略
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        // 获取课程id
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    // cleanData.print()
    // 统计今天到现在为止实战课程的访问量
    /**
      * x ==> ClickLog x就是ClickLog
      */
    cleanData.map(x => {
      // HBase rowkey设计: 20181111_01 时间_课程id
      // WordCount
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      // 一个一个的Partition写入效果最佳
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
