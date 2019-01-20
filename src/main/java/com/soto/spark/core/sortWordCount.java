package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class sortWordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("sortWordCount")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("/home/sotowang/Templates/datagrand_0517/candidate.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });


        JavaPairRDD<String, Long> pairs = words.mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String word) throws Exception {
                return new Tuple2<String, Long>(word, 1l);
            }
        });


        //以单词作为key,统计每个单词出现的次数
        //    使用reduceByKey算子
        JavaPairRDD<String, Long> wordCounts = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });


        JavaPairRDD<Long, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Long> tuple2) throws Exception {
                return new Tuple2<Long, String>(tuple2._2, tuple2._1);
            }
        });

        JavaPairRDD<Long, String> sortedCountWords = countWords.sortByKey(false);

        JavaPairRDD<String, Long> sortedWordCounts = sortedCountWords.mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<Long, String> tuple2) throws Exception {
                return new Tuple2<String, Long>(tuple2._2, tuple2._1);
            }
        });


        sortedWordCounts.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }
        });



        jsc.close();
    }
}
