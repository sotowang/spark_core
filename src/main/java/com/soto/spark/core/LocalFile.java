package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


/**
 * 使用本地文件创建RDD  LocalFile.java
 *
 *  案例: 统计文本文件字数
 */
public class LocalFile {


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        //针对本地文件创建RDD   textFile()方法
        JavaRDD<String> lines = jsc.textFile("hdfs://sotowang-pc:9000/input/hadoop002.pem");

        JavaRDD<Long> lineLength = lines.map(new Function<String, Long>() {
            public Long call(String line) throws Exception {
                return Long.valueOf(line.length());
            }
        });


        Long count = lineLength.reduce(new Function2<Long, Long, Long>() {
            public Long call(Long long1, Long long2) throws Exception {
                return long1 + long2;
            }
        });

        System.out.println("文件总字数是" + count);





        jsc.close();

    }







}
