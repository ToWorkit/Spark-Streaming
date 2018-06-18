package com.imooc.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者
 */
public class kafkaConsumer extends Thread{
    private String topic;

    public kafkaConsumer(String topic) {
        this.topic = topic;
    }

    private ConsumerConnector createConnector() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", kafkaProperties.zk);
        properties.put("group.id", kafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // get(0)
        topicCountMap.put(topic, 1);
//        topicCountMap.put(topic2, 1);
//        topicCountMap.put(topic3, 1);

        // String: topic
        // List<KafkaStream<byte[], byte[]>>: 对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);
        // 获取每次接收到的消息
        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("receive: " + message);
        }
    }
}
