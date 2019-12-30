package com.example;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.util.parsing.combinator.testing.Str;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class kafka80_Producer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
      //  props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String , String> producer = new Producer<String, String>(config);
        long events = Long.parseLong((args[0]));
        Random rnd = new Random();
        for(long nevents=0;nevents<events;nevents++){
            long runtime = new Date().getTime();
            String ip="192.168.2"+rnd.nextInt(255);
            String msg = runtime+",www.example.com,"+ip;
            KeyedMessage<String,String> data = new KeyedMessage<String, String>("page_visits",ip,msg);
            producer.send(data);
        }
        producer.close();
    }
}
