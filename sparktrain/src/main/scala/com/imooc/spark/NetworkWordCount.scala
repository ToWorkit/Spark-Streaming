package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Seconds

/**
  * Spark Streaming处理Socket数据
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    /**
      * 创建StreamingContext需要两个参数 SparkConf和batch interval
      */
    val sc = new StreamingContext(sparkConf, Seconds(5))

    val lines = sc.socketTextStream("localhost", 67789)
    // 处理数据
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    sc.start()
    sc.awaitTermination()
  }
}
