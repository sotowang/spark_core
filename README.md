# Spark基础
[【北风网】Spark 2.0从入门到精通（278讲）](https://www.bilibili.com/video/av19995678?t=267)

## WordCountLocal.java 使用java写的WordCount程序


* 创建SparkConf 对象,设置spark应用配置信息,
    使用setMaster()可以设置Spark应用程序要连接的的Spark集群的master节点的url
    但如果设置为local则代表在本地运行
    
```java
SparkConf sparkConf = new SparkConf()
            .setAppName("WordCountLocal")
            .setMaster("local");
```

* 创建JavaSparkContext对象
    在Spark中,SparkContex是spark所有功能的一个入口, 
    作用: 初始化spark应用程序主要核心组件,包括调度器(DAGScheuler,TaskScheduler),
    还会去到Spark Master 节点上进行注册
    
    
```java
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
```

* 要针对输入源(hdfs文件 本地文件) 创建一个初始RDD,输入源中的数据会被打散,
    分配到RDD的每个partition中,从而形成一个初始的分布式的数据集.
    案例使用本地输入源,SparkContext中,用于根据文件类型的输入源创建RDD的方法,叫做textFile()方法
    在Java中创建的普通RDD叫做JavaRDD
    
```java
    JavaRDD<String> lines = jsc.textFile("/home/sotowang/launchy.ini");

```

>每一个RDD相当于文件的每一行


* 对初始RDD进行transformation操作:
    通常操作会创建Function,并配合RDD的map,flatmap等算子来执行function,
    如果比较简单,则创建指定Function的匿名内部类
    如果比较复杂,则单独创建一个类作为实现接口的类

    >先将每一行拆分成单个单词
  
  >FlatMapFunction<输入,输出>
  
  >flatmap算子:将RDD的一个元素拆为多个元素
  
```java
  JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
  
              public Iterator<String> call(String line) throws Exception {
                  return Arrays.asList(line.split(" ")).iterator();
              }
  
  
          });
```  

* 将每个单词映射为(单词,1)的形式

> PairFunction(输入类型,输出类型1,输出类型2)

```java
JavaPairRDD<String, Long> pairs = words.mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String word) throws Exception {
                return new Tuple2<String, Long>(word, 1l);
            }
        });
```

* 以单词作为key,统计每个单词出现的次数
    使用reduceByKey算子
    
```java
JavaPairRDD<String, Long> wordCounts = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
```

* action操作 触发程序执行

```java
wordCounts.foreach(new VoidFunction<Tuple2<String, Long>>() {
            public void call(Tuple2<String, Long> wordcount) throws Exception {
                System.out.println(wordcount._1 + "出现了" + wordcount._2 + "次");
            }
        });

```

* 关闭
```java
jsc.close();
```
        





