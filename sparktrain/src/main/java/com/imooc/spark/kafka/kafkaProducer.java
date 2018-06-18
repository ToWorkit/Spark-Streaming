package com.imooc.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka生产者
 */
public class kafkaProducer extends Thread{
    private  String topic;

    private Producer<Integer, String> producer;

    public kafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();

        properties.put("metadata.broker.list", kafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        // 一般使用 1，严格使用 -1 查看 ProducerConfig 继承的父级源码得知
        properties.put("request.required.acks", "1");

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int messageNo = 1;

        while (true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);

            messageNo ++;

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
