package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondarySort {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("sortWordCount")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        JavaRDD<String> lines = jsc.textFile("/home/sotowang/Templates/secondarySortKey");

        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(
                new PairFunction<String, SecondarySortKey, String>() {
                    public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                        String[] lineSplited = line.split(" ");
                        SecondarySortKey secondarySortKey = new SecondarySortKey(Integer.valueOf(lineSplited[0]), Integer.valueOf(lineSplited[1]));
                        return new Tuple2<SecondarySortKey, String>(secondarySortKey,line);
                    }
                });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();

        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        sortedLines.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        jsc.close();
    }
}
