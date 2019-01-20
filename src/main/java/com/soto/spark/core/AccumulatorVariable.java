package com.soto.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加变量
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Accumulator")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        final Accumulator<Integer> sum = jsc.accumulator(0);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = jsc.parallelize(numberList);


        numbers.foreach(
                new VoidFunction<Integer>() {
                    public void call(Integer integer) throws Exception {
                        sum.add(integer);
                    }
                }
        );

        System.out.println(sum.value());

        jsc.close();
    }
}
