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


---

# Spark内核架构

spark-submit 
--> Driver(执行自己写的Application应用程序)   
--> SparkContext（sparkContext初始化的时候做的两件事：构造出DAGScheduler和TaskScheduler）          
--> DAGScheduler            
--> TaskScheduler (负责通过它对应的一个后台进程去连接Master，向Master注册Application)        
--> Master 接收到Application注册请求后，会使用自己的资源调度算法，在Spark集群的worker上，为这个Application启动多个Executor     
--> Executor启动后会反向注册到TaskScheduler（Driver？？）上   
--> 所有Executor都反向注册到Driver上之后，Driver结束SparkContext初始化，会继续执行我们所写的代码  
--> 每执行到一个action，就会创建一个Job，Job会提交给DAGScheduler  
--> DAGSCheduler 会将Job划分为多个stage（stage划分算法），然后每个stage创建一个TaskSet    
--> TaskScheduler将TaskSet里每一个task提交到executor执行（task分配算法）    
--> Executor每接收到一个task，都会用TaskRunner封闭task，然后从线程池里取一个线程，执行task      
--> Taskrunner将我们编写的代码，也就是要执行的算子及耿发，copy，反序列化，然后执行task      
    task有2种，ShuffleMapTask和ResultTask，只有最后一个stage是ResultTask，之前的stage，都是ShuffleMapTask      
    
--> 所以最后整个Spark应用程序的执行，就是stage分批次作为taskset提交到executor执行，每个task针对RDD的一个partition，执行我们定义的算子和函数


## 宽依赖（Shuffle Dependency）与窄依赖（Narrow Dependency）

* 窄依赖： 一个RDD，对它的父RDD，只有简单的一对一关系，即RDD的每个partition，仅仅依赖父RDD中的一个partition，父RDD
和子RDD的partition之间的对应关系是一对一的
  
* 宽依赖： 本质就是shuffle，每一个父RDD的partition中的数据，都可能会传输一部分，具有交互错综复杂的关系

## Spark的三种提交模式

* standalone模式

* yarn-cluster模式

* yarn-client模式

若要切换到第二或三种模式，在spark-submit脚本上--master参数，设置为yarn-cluster，或yarn-client，默认为standalone模式

--> spark-submit 提交（yarn-cluster）   
--> 发送请求到ResourceManager，请求启动ApplicationMaster（相当于Driver）  
--> ResourceManager 分配container在某个nodeManager上，启动ApplicationMaster  
--> AM找到RM，请求container，启动executor   
--> AM连接其它NM，来启动executor，这里NM相当于worker  
--> executor启动后，向AM反向注册

### yarn-cluster与yarn-client

* 1. yarn-client用于测试，因为driver运行在本地客户端，负责调度application，会与yarn集群产生大量的网络通信，从而导致网卡流量激增
可能会被公司运维警告，好处在于，直接执行时，本地可以看到所有的log，方便调试
* 2. yarn-cluster，用于生产环境，以为driver运行在nodemanager，没有网卡済激增问题
缺点在于，调度不方便，本地用spark-submit提交后，看不到log，只能通过yarn application-logs application id 这种命令查看


---

## SparkContext 原理剖析

--> sparkcontext（）  
--> createTaskScheduler() (其实就是TaskScheduler)

```markdown
TaskSchedulerImpl,  
SparkDeploySchedulerBackend()
(它在底层接收TaskSchedulerImpl的控制,实际上负责Master的注册,Executor的反注册,task发送到Executor等操作)
创建SchedulePool,它有不同的优先策略,比如FIFO
```   
--> TaskSchedulerImpl的start     
--> SparkDeploySchedulerBackend()的start     
--> Appclient       
--> 创建一个ClientActor     
--> registerWithMaster()->tryRegesterAllMasters()       
--> RegisterApplication(case class,里面封闭了Application的信息)     
--> Spark集群,Master->worker->executor
--> 反向注册到SparkDeploySchedulerBackend()上

---

# Spark 性能调优

## 诊断内存的消耗

### 内存都花费在哪里了


* 每一个Java对象,都有一个对象头,会占用16个字节,主要包括一些对象的元信息,比如指向它的类的指针,如果一个对象本身很小,如比就包括了int类型的field,那么其对象头比对象还大


对象头

```markdown
在32位系统上占用8bytes，64位系统上占用16bytes。
```

* Java的String对象,会比它内部的原始数据,多出40个字节.
因为它内部使用char数组来保存内部的字符序列,并且还得保存诸如数组长度之类的信息.而且因为String使用的是UTF-8编码,
所以每个字符会占用2个字节.比如10个字符的String,会占用60个字节

* Java中的集合类型,比如HashMap和LinkedList,内部使用的是链表数据结构,所以对链表中的每一个数据,都使用了Entry对象来包装.
Entry不光有对象头,还有指向下一个Entry的指针,通常占用8个字节

* 元素类型为原始数据类型的集合(如int),内部通常会使用原始数据类型的包装类型,比如integer来存储元素

```markdown
Java定义了八种基本类型的数据：byte，short，int，long，char，float，double和boolean。
```

### 如何判断程序消耗了多少内存

* 首先,自己设置RDD的并行度,有两种方式:1. 在parallelize(),textFile()等方法中传入第二个参数,设置RDD的task/
partition的数量. 2.用SparkConf.set()方法设置一个参数,spark.default.parallelism,可以统一设置这个application所有RDD的partition数量

* 在程序中将RDD cache 到内存中,调用RDD.cache() 方法

* 观察Driver的log,会发现类似于: "INFOBlockManagerMasterActor.Added rdd_0_1 in memory on mbk.local:50311(size:717.5 kb,free:332.3MB)"
的日志信息,这就显示了每个partition占用了多少内存

* 将这个内存信息剩以partition数量,可得RDD内存占用量


## 高性能序列化类库

* Java序列化机制(默认)

* Kryo序列化机制

```java
new SparkConf().set("saprk.serializer","org.apache.spark.serializer.KyroSerializer")
```

使用Kryo时,它要求是需要序列化的类,要预先进行注册,以获得最佳性能,如果不注册的话,那么Kryo必须时文保存类型的全限定名,反而占用不少内存.
Spark默认是对Scala中常用的类型自动注册了Kryo的,都在AllScalaRegistry类中     

但是,比如自己的算子中,使用了外部的有很大定义类型的对象,那么还是需要进行注册

> 如果要实现自定义类型,使用如下代码

```java

SparkConf conf = new SparkConf().setMaster(...).setAppName(...);
conf.registerKryoClasses(Counter.class);
JavaSparkContext sc = new JavaSparkContext(conf);

```

### 优化Kryo类库的使用

* 优化缓存大小

>如果注册的要序列化的自定义类型,本身特别大,比如包含了超过100个field,那么就会导致要序列化和对象过大.此时就需要对Kryo本身进行优化.
因为Kryo内部的缓存可能不够存放那么大的class对象.此时就需要调用SparkConf.set()方法,设置spark.kryoserializer.buffer.mb参数值,将其调大

>默认情况下值为2,最大能缓存2M的对象,然后进行序列化,可以将其在必要时调大,比如10

* 预先注册自定义类型

> 虽然不注册自定义类型,Kryo类库也能正常工作,但女士们的话,对天它要序列化的每个对允是,都会保存一份它的全限定类名.
此时反而会耗费大量内存.因此通常都建议预先注册好要序列化的自定义的类.

### 在什么场景下使用Kryo类库

*  算子函数使用到了外部的大数据的情况

>如:我们在外部定义了一个封闭了应用所有配置的对象,比如自定义了一个Myconfiguration对象,里面包含了100m的数据.
然后在算子函数里面,使用到了该对象

> 此时使用默认的Java序列化机制,会导致序列化速度缓慢,且序列化后数据还是比较大,比较占用内存空间


### 优化数据结构

* 优先使用数组及字符串,而不是集合类,即优先拿手Array,而不是ArrayList,LinkedList,HashMap等集合

```markdown
比如List<Integer> list = new ArrayList<Integer>(),将其替换为int[] arr= new int[].
这样的话 array既比List少了额外的信息存储开销,还能使用原始数据类型(int) 来存储数据,比List 中用Integer这种包装类型存储数据,要节省内存的多

```

```markdown
比如 通常企业级应用的做法是,对于HashMap,List这种数据,统一用String拼接成特殊格式的字符串,
比如Map<Integer,Person> persons = new HashMap<Integer,Person>().
可以优化为特殊的字符串格式:
    id,name,address|id,name,address...

```

* 避免使用多层嵌套的对象结构.

比如,

```java
public class teacher{
    private List<Student> students = new ArrayList<Student>()
}
```

就是非常不好的例子,对于上述例子完全可以彩特殊的字符串来进行存储,比如使用json字符串来存储数据

```json
{"teacherId":1,
"teacherName":"leo",
"students":[{
"studentId":1,
"studentName":"tom"
}]}
```

* 尽量使用int替代String

String虽然比ArrayList,HashMap等数据结构高效,但还有额外信息消耗.比如之前用String表示id,那么现在完全可以用数字类型的int来进行代替

这里提醒,在Spark应用中,id就不要用uuid了,因无法转成int,用自增的int类型的id即可


## JVM垃圾回收调优


### 优化Executor内存

* 默认情况下,Executor的内存空间,40%给task,存放它在运行期间动态创建的对象,
划了60%给RDD缓存,比如说我们执行RDD.cache()的时候,Executor上的RDD
的 partition会占用60%空间缓存.

>会导致内存空间很快填满,频繁触发GC,致task线程频繁停止,降低性能

```java
new Sparkconf().set("spark.storage.memoryFraction","0.5")

```

> 注: 比例越低,task占比越大


* Executor中分配给task的内存空间,就是分配给task的jvm堆空间

>新生代+老年代

> 新生代:Eden区域+survivor1+survivor2

首先创建的对象都是放入Eden和survivor1的,Survivor作为备用.Eden区域满了之后,触发minor gc操作,会回收新生代中不再使用的
对象,此时Eden和Survivor1中的存活对象移入Survivor2区域.移完后,Eden和Survivor1中剩余的,就是不再使用的对象,那么gc将他们移除内存空间
之后Survivor1和Survivor2角色调换,Survivor1变备用.

> 如果一个对象在新生代,多次minor gc都存活,说明它是长时间存活的对象,会将它移入老年代

若将Eden和Survivor1中的存活对象移入Survivor2区域时发现Survivor2内存不够用,则会将对象移入老年代,则会有短时间存活的对象进入老年代,占用内存空间,
老年代会因此被快速占满,会触发Full GC,回收老年代对象.导致Full GC(特别慢)频繁发生,task线程频繁停止.


* task期间,大量Full gc 发生,说明年轻代的Eden区间不够大

```markdown
1. 降低spark.storage.memoryFraction比例,给年轻代更多空间,存放短时间戚对象
2. 给Eden区域分配更大空间,使用-Xmn,通常建议给Eden区域预计大小的3/4
3. 如果使用HDFS文件,那么很好估计Eden大小,如果每个executor有4个task,然后每个hdfs压缩块解压缩后大小是3倍,此外每
个hdfs块大小为64M,那么Eden区域预计大小为 4*3*64,再通过-Xmn对数,将Eden区域大小设置为 4*3*64*3/4


```























