package com.imooc.spark.project

import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {
    //设置Hadoop的环境变量
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3\\hadoop-2.7.3")

      // sc
      val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")
      val ssc = new StreamingContext(sparkConf, Seconds(60))

      //指定Topic信息
      val topic = Set("test")

      //直接读取Broker，指定就是Broker的地址
      val brokerList = Map[String,String]("metadata.broker.list"->"192.168.187.116:9092")

      //创建一个DStream                          key    value   key的解码器  value的解码器
      val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,brokerList,topic)

    //读取消息
    val message = lines.map(e=>{
        new String(e.toString())
      }
    )

    message.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
