package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * 广播变量
 */
public class BroadcastVariable {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BroadcastVariable")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        final int factor = 3;

        final Broadcast<Integer> factorBroadcast = jsc.broadcast(factor);



        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numbers = jsc.parallelize(numberList);


        //让集合中的每个数都剩以外部factor
        JavaRDD<Integer> mutipleNum = numbers.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                int factor = factorBroadcast.value();
                return integer * factor;
            }
        });


        mutipleNum.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);

            }
        });



        jsc.close();
    }


}
