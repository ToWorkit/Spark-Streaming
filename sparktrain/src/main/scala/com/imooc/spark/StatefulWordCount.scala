package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成有状态统计
  */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    // 创建sc
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint(检查点)
    // 生产环境中，需要将checkpoint设置到HDFS的某个文件夹中
    sc.checkpoint(".")

    // 监听
    val lines = sc.socketTextStream("192.168.187.116", 6789)

    // 数据处理
    val result = lines.flatMap(_.split(" ")).map((_, 1))
    val state = result.updateStateByKey[Int](updateFunc _)

    state.print()

    // 手动关闭
    sc.start()
    sc.awaitTermination()
  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues 当前的
    * @param preValues  老的
    * @return
    */
  def updateFunc(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
