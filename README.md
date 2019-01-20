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
        

----

## WordCountCluster.java 将Java开发的应用程序 部署到Spark集群上运行

 如果要在集群上运行 需要修改:
 
 * 将SparkConf的setMaster() 方法删掉,默认它自己会去连接
    
```java
SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountCluster");
```
 
 * 不使用本地文件,  修改为hadoop hdfs上的存储大数据的文件   

```java
JavaRDD<String> lines = jsc.textFile("hdfs://sotowang-pc:9000/input/hadoop002.pem");

```

* 将hadoop002.pem 文件上传到hdfs上

* 使用maven插件对maven工程打包

[使用Intellij Idea打包java为可执行jar包](https://blog.csdn.net/xuemengrui12/article/details/74984731)


* 编写spark-submit 脚本

```bash
spark-submit 
--class com.soto.spark.core.WordCountCluster
--num-executors 3
--driver-memory 100m
--executor-memory 100m
--executor-cores 3
/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/spark-train/out/artifacts/spark_train_jar/spark-train.jar


```

* 执行sprk-submit脚本到集群执行



---

# Spark架构原理

* Driver (是一个进程)

```markdown
1. 我们编写的Spark应用程序在Driver上,由Driver进程执行
2. Spark集群的节点之一  --->就是你提交Spark程序的机器
3. Driver进程启动后,会进行初始化操作,在这个过程中,会发送请求到Master上,进行Spark应用程序的注册(就是让Master知道有新的Spark应用程序在运行)
4. Driver注册了一些Executor之后,可以正式执行Spark应用程序了

```
正式执行Spark应用程序的步骤

> 1. 创建初始RDD,读取数据源 (HDFS文件被读取到多个worker节点上,形成内存中的分布式数据集,也就是初始RDD)
> 2. Driver会根据我们对RDD定义的操作,提交一大堆task去Executor上 


* Master (是一个进程)

```markdown
1. 负责资源的调度,集群监控
2. 在接收到Spark应用程序注册请求后,发送请求给worker,进行资源的调度和分配(其实:资源分配就是Executor的分配)

```


* Worker(是一个进程)

```markdown
1. 用自己的内存存储RDD的某个或某些partition
2. 启动其它进程和线程
3. 对RDD上的partition进行处理和计算
4. worker接收到Master请求后,会为Spark应用启动Executor

```

* Executor(是一个进程)

```markdown
1. 由worker启动,Executor启动后,会向Driver反注册,这样Driver就会知道 哪些Executor是为它进行服务了

```



* Task (是一个线程)

```markdown
1. 由Executor启动,task会对RDD的partition数据执行指定的算子操作,形成新的RDD partition

```



> Executor 和 Task 对RDD的partition进行并行计算,执行我们对RDD定义的map  flatmap reduce等操作


---

# 创建RDD

## 并行化集合创建RDD  ParallelizeCollection.java

### 案例:求1-10之和

```java
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);

int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });
```

## 使用本地文件创建RDD  LocalFile.java

### 案例: 统计文本文件字数

针对本地文件创建RDD   textFile()方法

```java
//针对本地文件创建RDD   textFile()方法
        JavaRDD<String> lines = jsc.textFile("/home/sotowang/user/note/other/data");

        JavaRDD<Long> lineLength = lines.map(new Function<String, Long>() {
            public Long call(String line) throws Exception {
                return Long.valueOf(line.length());
            }
        });


        Long count = lineLength.reduce(new Function2<Long, Long, Long>() {
            public Long call(Long long1, Long long2) throws Exception {
                return long1 + long2;
            }
        });

        System.out.println("文件总字数是" + count);
```


## 使用HDFS文件创建RDD HDFSFile.java

```java
JavaRDD<String> lines = jsc.textFile("hdfs://sotowang-pc:9000/input/hadoop002.pem");
```

---
# 常用transformation

* map
* filter
* flatMap
* groupByKey
* reduceByKey
* sortByKey
* join
* cogroup

# 常用action

* reduce
* collect
* count
* take(n)
* saveAsTextFile
* countByKey
* foreach

---

# transformation操作实战 TransformationOperation.java

## map算子案例: 将集合中每个元素都剩以二

## filter算子案例: 过滤集合中的偶数
如果想在新的RDD中保留元素,则返回true

## flatMap案例:将文本行拆分为多个单词

## groupByKey案例: 按照班级对成绩进行分组
groupByKey算子,返回的还是JavaPairRDD
但是,JavaPairRDD的第一个泛型类型不变,第二个泛型类型变成Iterable集合类型
也就是谘,按照key进行分组,形成多个value

## reduceByKey案例:统计每个班级的部分

## sortByKey案例  按照学生分数进行排序

## join和cogroup案例： 打印学生成绩

--- 

# action操作实战 actionOperation.java

## reduce案例： 对集合中数字累加

## collect案例： 将集合数字剩以2 
因其将远程RDD拉取到本地进行操作，因而不建议使用该方法，可能会造成OOM异常

## count案例： 统计有多少个元素

## take案例
与collect类似，从远程获取RDD，但collect获取所有数据，take获取前N个数据


## saveAstextFile

## countByKey

## foreach

---

# RDD持久化机制  Persist.java

cache() 或 persist使用

> 必须在transformation 或者textFile创建了一个RDD之后，直接连续调用cache（）
或persist（）才可以，若先创建RDD，别起一行cache（）或persist（）是没用的，且会报错，使文件丢失

## 持久化策略（尽量不使用DISK）

* MEMOEY_ONLY

> 使用级别1

* MEMORY_AND_DISK

* MEMORY_ONLY_SER

> 使用级别 2

> 序列化+ 反序列化 减少内存开销，但加大CPU开销

* MEMOEY_AND_DISK_SER

* DISK_ONLY

* MEMORY_ONLY_2MEMORY_AND_DISK_2

> 2表示持久化后数据会复制一份 丢失后直接使用备份

# 共享变量

> 默认情况下，算子函数内，使用的外部变量，会copy到执行这个函数的每一个task中

> 如果变量特别大的话，那么网络传输也会增大，在每个节点上占用的空间也会特别大

共享变量只会将变量在每个节点copy一份，让节点上的所有task使用


## Broadcast Variable （只读） BroadcastVariable.java

```java
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
```


## Accumulator （可写，因而可以累加）  AccumulatorVariable.java

task不能读Accumulator的值，Driver可以

```java
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
```

---

# 基于排序机制和wordCount程序  sortWordCount.java

# 二次排序 

SecondarySortKey.java

```java
package com.soto.spark.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义 二次排序的key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    private int first;
    private int second;
    
    
    public int compare(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else  {
            return this.second - that.getSecond();
        }
    }

    public boolean $less(SecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $less$eq(SecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }

        return false;
    }

    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    public int compareTo(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else  {
            return this.second - that.getSecond();
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}


```

```markdown
1. 实现自定义的key，要实现order接口和serializable接口，在key中实现对多个列的排序算法
2. 将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
3. 使用sortByKey算子按照自定义的key进行排序
4. 再次映射，剔除自定义的keey，只保留文本行

```

--- 

# 取TopN案例

* Top3.java


* 分组取topN，案例：每个班级排名前3的成绩  GroupTop3.java


