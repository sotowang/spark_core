package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 *
 */
public class Persist {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("/home/sotowang/user/note/other/access.log")
                .cache();



        long beginTime = System.currentTimeMillis();
        long count = lines.count();

        System.out.println(count);


        long endTime = System.currentTimeMillis();

        System.out.println("cost: " + (endTime - beginTime) + "  milliseconds");



         beginTime = System.currentTimeMillis();
         count = lines.count();

        System.out.println(count);


         endTime = System.currentTimeMillis();

        System.out.println("cost: " + (endTime - beginTime) + "  milliseconds");




        jsc.close();
    }
}
