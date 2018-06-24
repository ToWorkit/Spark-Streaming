/*
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer


/**
  * Spark Streaming对接Kafka
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {

    // sc
    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.187.116:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test"
    )

    val topics = Array("streamingtopic")

    // Spark Streaming 对接Kafka
    val messages = KafkaUtils.createDirectStream[String, String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // TODO... 自己去测试为什么要取第二个
    messages.map(record => (record.key, record.value)).print()

    sc.start()
    sc.awaitTermination()
  }
}
*/
