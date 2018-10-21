package com.soto.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KProducer extends Thread{

    private String topic;

    private Producer<Integer, String> producer;

    public KProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperities.BROKER_LIST);
//        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(properties);
    }


    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message" + messageNo;
            producer.send(new ProducerRecord<Integer, String>(KafkaProperities.TOPIC, message));
            System.out.println("Sent: " + message);
            messageNo ++;

            try {
                Thread.sleep(2000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
