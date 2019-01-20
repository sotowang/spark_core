package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("sortWordCount")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("/home/sotowang/Templates/top");

        JavaPairRDD<Integer, String> numbers = lines.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortedNum = numbers.sortByKey(false);

        JavaRDD<Integer> sortedNums = sortedNum.map(new Function<Tuple2<Integer, String>, Integer>() {
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });

        List<Integer> sortedNUmList = sortedNums.take(3);


        for (Integer num : sortedNUmList) {
            System.out.println(num);
        }


        jsc.close();
    }
}
