package com.soto.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformation操作实战
 */
public class TransformationOperation {

    public static void main(String[] args) {
//        map();


//        filter();


//        flatMap();
//        groupByKey();


//        reduceByKey();

//        sortByKey();


        joinAndCogroup();


    }



    /**
     * map算子案例: 将集合中每个元素都剩以二
     */

    private static void map() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("map")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);

        JavaRDD<Integer> multipleNumber = numberRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        multipleNumber.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        jsc.close();
    }


    /**
     * filter算子案例: 过滤集合中的偶数
     */
    private static void filter() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);


        //如果想在新的RDD中保留元素,则返回true
        JavaRDD<Integer> evenNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });


        evenNumberRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.close();
    }


    /**
     * flatMap案例:将文本行拆分为多个单词
     */
    private static void flatMap() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        JavaRDD<String> lines = jsc.parallelize(lineList);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }

        });


        words.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        jsc.close();

    }


    /**
     * groupByKey案例: 按照班级对成绩进行分组
     */
    private static void groupByKey() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("groupByKey")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 56),
                new Tuple2<String, Integer>("class2", 48)
        );

        JavaPairRDD<String, Integer> scores = jsc.parallelizePairs(scoreList);

        JavaPairRDD<String,Iterable<Integer>> groupScores = scores.groupByKey();

        groupScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> iterator) throws Exception {
                System.out.println("class:+   " + iterator._1);
                Iterator<Integer> ite = iterator._2.iterator();
                while (ite.hasNext()) {
                    System.out.println(ite.next());
                }


                System.out.println("#################################");
            }
        });





        jsc.close();


    }


    /**
     * reduceByKey案例:统计每个班级的部分
     */
    private static void reduceByKey() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("reduceByKey")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 56),
                new Tuple2<String, Integer>("class2", 48)
        );

        final JavaPairRDD<String, Integer> scores = jsc.parallelizePairs(scoreList);


        JavaPairRDD<String, Integer> sumScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        sumScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> score) throws Exception {
                System.out.println(score._1 + "   sum: " + score._2);

            }
        });



        jsc.close();
    }


    /**
     * sortByKey案例  按照学生分数进行排序
     */
    private static void sortByKey() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("sortByKey")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(65, "a"),
                new Tuple2<Integer, String>(68, "b"),
                new Tuple2<Integer, String>(98, "c"),
                new Tuple2<Integer, String>(78, "d")
        );

        final JavaPairRDD<Integer, String> scores = jsc.parallelizePairs(scoreList);


        JavaPairRDD<Integer,String > sortScores = scores.sortByKey(false);

        sortScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> score) throws Exception {
                System.out.println(score._2 + "   : " + score._1);

            }
        });



        jsc.close();
    }

    /**
     * join和cogroup案例： 打印学生成绩
     */
    private static void joinAndCogroup() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("joinAndCogroup")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(2, "b"),
                new Tuple2<Integer, String>(3, "c"),
                new Tuple2<Integer, String>(4, "d")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 80),
                new Tuple2<Integer, Integer>(4, 70)
        );





        final JavaPairRDD<Integer, String> students = jsc.parallelizePairs(studentList);
        final JavaPairRDD<Integer, Integer> scores = jsc.parallelizePairs(scoreList);

        JavaPairRDD<Integer,Tuple2<String, Integer>> studenScores = students.join(scores);

        studenScores.foreach(
                new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
                    public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple2) throws Exception {
                        System.out.println("studentId:  " + tuple2._1);
                        System.out.println("studentName:  " + tuple2._2._1);
                        System.out.println("studentScore:  " + tuple2._2._2);
                    }
                }
        );





        jsc.close();
    }
}
