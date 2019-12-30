package com.example;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerTest implements Runnable {
    private KafkaStream<byte[],byte[]> kafkaStream;
    private int threadNumber;
    public ConsumerTest(KafkaStream<byte[],byte[]> kafkaStream,int threadNumber){
        this.kafkaStream = kafkaStream;
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
       ConsumerIterator<byte[],byte[]> it =  kafkaStream.iterator();
       while(it.hasNext()){
           System.out.println("Thread: "+threadNumber+new String(it.next().message()));
       }
        System.out.println("Shutting down thread: "+threadNumber);
    }
}
