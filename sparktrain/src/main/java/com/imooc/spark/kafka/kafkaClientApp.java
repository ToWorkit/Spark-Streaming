package com.imooc.spark.kafka;

/**
 * kafka java api测试
 */
public class kafkaClientApp {
    public static void main(String[] args) {
        new kafkaProducer(kafkaProperties.TOPIC).start();

        new kafkaConsumer(kafkaProperties.TOPIC).start();
    }
}
