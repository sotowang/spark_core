package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);



        //要通过并行化集合的方式创建RDD,那么就调用SparkContext以及其子类的paralleliize()方法
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);


        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        System.out.println(sum);


        jsc.close();
    }










}
