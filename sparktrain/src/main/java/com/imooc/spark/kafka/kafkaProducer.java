package com.imooc.spark.kafka;

import kafka.javaapi.producer.Producer;

/**
 * kafka生产者
 */
public class kafkaProducer {
    private  String topic;

    private Producer<Integer, String> producer;

    public kafkaProducer(String topic) {
        this.topic = topic;

        producer = new Producer<Integer, String>();
    }
}
