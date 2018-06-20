package com.imooc.spark


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理文件系统(local/hdfs)的数据
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    //设置Hadoop的环境变量
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3\\hadoop-2.7.3")
    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(5))

    val lines = sc.textFileStream("C:\\Users\\Just Do It\\Desktop\\data\\imooc_spark_streaming\\data\\")

    // 处理数据
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    sc.start()
    sc.awaitTermination()
  }
}
