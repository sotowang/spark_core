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

/**
 * 将Java开发的应用程序 部署到Spark集群上运行
 */
public class WordCountCluster {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountCluster");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        JavaRDD<String> lines = jsc.textFile("hdfs://sotowang-pc:9000/input/hadoop002.pem");

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


        JavaPairRDD<String, Long> wordCounts = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> wordcount) throws Exception {
                System.out.println(wordcount._1 + "出现了" + wordcount._2 + "次");
            }
        });

        jsc.close();

    }







}
