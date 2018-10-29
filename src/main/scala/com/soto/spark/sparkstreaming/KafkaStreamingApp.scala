package com.soto.spark.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming 对接Kafka的方式一
  *
  * 未测试成功？？？？
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.print("Usage: KafkaStreamingApp <brokers>  <topics>")
      System.exit(1)
    }

    val Array(brokers,topics) = args

    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = topics.split(",").toSet
    //TODO ... Sparking Streaming如何对接Kafka
    val messages = KafkaUtils.createDirectStream[String,String](
      ssc,PreferConsistent,
      Subscribe[String,String](topicsSet,kafkaParams)
    )

    messages.map(record=>record.value()).count().print()
    //TODO... 测试为什么要取第二个
//    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()



    ssc.start()
    ssc.awaitTermination()


  }
}
