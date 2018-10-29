package com.soto.spark.project

import com.soto.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.soto.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.soto.spark.project.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object StatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.print("Usage: StatStreamingApp <brokers>  <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("StatStreamingApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = topics.split(",").toSet

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    //测试步骤-：测试数据接收
    //    messages.map(record => record.value()).count().print()

    //    测试步骤二：数据清洗
    val logs = messages.map(record => record.value())
    val cleanData = logs.map(line => {
      val infos = line.split("\t")

      //      infos(2) = "GET /class/128.html HTTP/1.1"
      //      url = /class/128.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      if (url.startsWith("/class")) {
        val courseHTML = url.split("/")(2)
        courseId = courseHTML.substring(0, courseHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)


    //    cleanData.print()


    //    测试步骤三：统计到现在为止实战课程的访问量
    cleanData.map(x => {
      //      HBase rowKey设计： 20181111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    //    测试步骤四：统计搜索引擎过来的到现在为止实战课程的访问量

    cleanData.map(x => {
      /**
        * http://cn.bing.com/search?q=Hadoop基础
        *
        * ==>
        * http:/cn.bing.com/search?q=Hadoop基础
        **/

      val referer = x.refer.replaceAll("//", "/")
      val splits = referer.split("/")

      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }
      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0, 8) + "_" + x._1 + "_" + x._2,1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
