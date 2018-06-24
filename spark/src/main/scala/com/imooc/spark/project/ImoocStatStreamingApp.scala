package com.imooc.spark.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(60))

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
      val courseId = 0
      // 清洗 必须是以 /class 开头的数据，其他数据忽略
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
