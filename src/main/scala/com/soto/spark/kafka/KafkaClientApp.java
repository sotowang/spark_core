package com.soto.spark.kafka;

public class KafkaClientApp {

    public static void main(String[] args) {
        new KProducer(KafkaProperities.TOPIC).start();

        new KConsumer(KafkaProperities.TOPIC).start();
    }
}
