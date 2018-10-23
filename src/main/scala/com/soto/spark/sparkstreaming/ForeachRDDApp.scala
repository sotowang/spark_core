package com.soto.spark.sparkstreaming

import java.sql.DriverManager

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到mysql中
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")

    /**
      * 创建StreamingContext需要两个参数：SparkConf 和 batch interval
      */
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //如果使用了stateful的算子，必须要设置checkpoint
    //在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))

//    state.print()  //将结果输出到控制台

    /**
      * 创建表：
         create table wordcount(
         word varchar(50) default null,
         wordcount int(10) default null
         );
      */
    //TODO... 将结果写入到MYSQL
//    result.foreachRDD(
//      rdd =>{
//        val connection = createConnection()  // executed at the driver
//        rdd.foreach { record =>
//          val sql = "insert into wordcount(word wordcount) values('" + record._1 + "'," + record._2 + "')"
//          connection.createStatement().execute(sql)
//        }
//      })

    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecord =>{
//        if(partitionOfRecord.size>0){
          val connection = createConnection()
          partitionOfRecord.foreach(record =>{
            val sql = "insert into wordcount(word,wordcount) values('" + record._1 + "','" + record._2 + "')"
            connection.createStatement().execute(sql)
          })
          connection.close()
//        }
      })
    })





    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 获取MySQL的连接
    * @return
    */
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark","root","123456")

  }

}
