package com.soto.spark.kafka;


import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

/**
 * Kafka消费者
 */
public class KConsumer extends Thread{

    private String topic;
    private Consumer consumer;
    public KConsumer(String topic){
        this.topic = topic;
        Properties properties = new Properties();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperities.BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaProperities.GROUP_ID);


        consumer = new KafkaConsumer(properties);


    }


    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(20);
            for (ConsumerRecord<Integer,String> record:records){
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
            try {
                Thread.sleep(4000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
