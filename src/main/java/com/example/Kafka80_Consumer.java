package com.example;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Kafka80_Consumer {
    private  ConsumerConnector consumerConnector;
    private String topic;
    private ExecutorService executorService;

    public Kafka80_Consumer(String a_zookeeper,String a_groupId,String topic){
        consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,a_groupId));
        this.topic = topic;

    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect",a_zookeeper);
        properties.put("group.id",a_groupId);
        properties.put("zookeeper.session.timeout.ms","400");
        properties.put("zookeeper.sync.time.ms","200");
        properties.put("autocommit.interval.ms","1000");
        return new ConsumerConfig(properties);
    }

    public void run(){
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,4);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap=  consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[],byte[]>> kafkaStreams = consumerMap.get(topic);
        executorService = Executors.newFixedThreadPool(4);
       int threadNumber=0;
      for(KafkaStream<byte[],byte[]> kafkaStream:kafkaStreams){
          executorService.submit(new ConsumerTest(kafkaStream,threadNumber));
          threadNumber++;
        }

    }

    public static void main(String[] args) {
        String zookeeper = "localhost:2181";
        String topic = "page_visits";
        String group_id = "test_group";
        Kafka80_Consumer consumer = new Kafka80_Consumer(zookeeper,group_id,topic);
        consumer.run();
        while (true) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                consumer.shutdown();
            }
        }
    }

    private void shutdown() {
        if(consumerConnector != null) consumerConnector.shutdown();
        if(executorService != null) executorService.shutdown();
        try {
            if(!executorService.awaitTermination(5000,TimeUnit.MILLISECONDS)){
                System.out.println("Timed out when trying to terminate");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Timed out when trying to terminate");
        }
    }
}
