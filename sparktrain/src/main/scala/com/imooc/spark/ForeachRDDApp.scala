package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计,并将结果写入到MySQL
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {
    // 创建sc
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(5))

    // 监听
    val lines = sc.socketTextStream("192.168.187.116", 6789)

    // 数据处理
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "', " +  record._2 +")"
          // 写入
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })


    // 手动关闭
    sc.start()
    sc.awaitTermination()
  }

  /**
    * 获取MySQL连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "LocalHost_1")
  }

}

