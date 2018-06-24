package com.imooc.spark.project

import kafka.serializer.StringDecoder
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

    //指定Topic信息：从streamingtopic的topic中，每次接受一条消息
    val topic = Map("streamingtopic" -> 1)

    //创建Kafka输入流，基于Receiver方式，链接到ZK
    val messages = KafkaUtils.createStream(ssc,"192.168.187.116:2181","test",topic)

    //接受数据，并处理
    messages.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
