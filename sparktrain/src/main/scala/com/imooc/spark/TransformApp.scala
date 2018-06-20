package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val sc = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 构建黑名单
      */
    val blacks = List("li", "op")
    // RDD
    val blacksRDD = sc.sparkContext.parallelize(blacks).map(x => (x, true))

    /**
      * (op,true)
      * (li,true)
      */
    // blacksRDD.foreach(println)

    val lines = sc.socketTextStream("192.168.187.116", 6789)
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        // 参考index.txt
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

    clicklog.print()

    sc.start()
    sc.awaitTermination()
  }
}
