package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * action操作实战
 */
public class actionOperation {

    public static void main(String[] args) {
//        reduce();

//        collect();


        count();



    }


    /**
     * 对集合中数字累加
     */
    private static void reduce() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);

        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);


        jsc.close();

    }

    /**
     * 对集合中数字X2
     */
    private static void collect() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("collect")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);

        JavaRDD<Integer> multipleNum = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });


       List<Integer> muti =  multipleNum.collect();


        System.out.println(muti);

        jsc.close();

    }



    private static void count() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("count")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);

        long count = numberRDD.count();


        System.out.println(count);

        jsc.close();


    }
}
