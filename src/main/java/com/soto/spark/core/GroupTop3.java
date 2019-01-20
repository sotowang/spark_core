package com.soto.spark.core;

import kafka.controller.LeaderIsrAndControllerEpoch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 分组取top3
 */
public class GroupTop3 {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("sortWordCount")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("/home/sotowang/Templates/top");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] splited = s.split(" ");
                        return new Tuple2<String, Integer>(splited[0], Integer.valueOf(splited[1]));
                    }
                }
        );

        JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Scores = groupPairs.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                        Integer[] top3 = new Integer[3];

                        String className = tuple2._1;
                        Iterator<Integer> scores = tuple2._2.iterator();

                        while (scores.hasNext()) {
                            Integer score = scores.next();
                            for (int i = 0; i < 3; i++) {
                                if (top3[i] == null) {
                                    top3[i] = score;
                                    break;
                                } else if (score > top3[i]) {
                                    for (int j = 2; j > 1; j--) {
                                        top3[j] = top3[j - 1];
                                    }
                                    top3[i] = score;

                                    break;

                                }
                            }
                        }

                        return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
                    }

                }
        );


        top3Scores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println("class:  " + tuple2._1);
                Iterator<Integer> scores = tuple2._2.iterator();
                while (scores.hasNext()) {
                    Integer score = scores.next();
                    System.out.println(score);
                }
                System.out.println("================================");
            }

        });





        jsc.close();
    }


}
