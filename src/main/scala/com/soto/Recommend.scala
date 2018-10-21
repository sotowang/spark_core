//package com.soto
//import java.io.File
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//
//object Recommend {
//  def main(args: Array[String]): Unit = {
//
//  }
//    def recommend(model:MatrixFactorizationModel,movieTitle:Map[Int,String])={
//      var choose = ""
//      while(choose !=3) {
//        //如果选择3.离开,就结束程序
//        print("请选择要推荐类型 1.针对用户推荐电显 2.针对电影推荐给感兴趣的用户 3.离开?")
////        choose = readLine()  //读取用户输入
//        choose = readLine()  //读取用户输入
//        if(choose == "1"){  // 如果输入1,针对用户推荐电显
//          print("请输入用户id?")
//          val inputUserID = readLine()  //读取用户ID
//          RecommendMovies(model,movieTitle,inputUserID.toInt)  //针对此用户推荐电影
//        }else if (choose =="2"){
//          println("请输入电影的id?")
//          val inputMovieID = readLine()
//          RecommendUsers(model,movieTitle,inputMovieID.toInt)  //针对此电影推荐用户
//
//        }
//      }
//    }
//
//    def SetLogger = {
//      Logger.getLogger("org").setLevel(Level.OFF)
//      Logger.getLogger("com").setLevel(Level.OFF)
//      System.setProperty("spark.ui.showConsoleProgress","false")
//      Logger.getRootLogger().setLevel(Level.OFF)
//    }
//
//    def PrepareData():(RDD[Rating],Map[Int,String])={
//      //1.创建用户评分数据
//      val sc = new SparkContext(new SparkConf().setAppName("Recommend").setMaster("local[4]"))
//
//      println("开始读取用户评分数据中...")
//
//      val DataDir = "/home/sotowang/Templates/ml-100k"
//
//      val rawUserData = sc.textFile(new File(DataDir,"u.data").toString)
//      println(rawUserData.take(5))
//      val rawRatings = rawUserData.map(_.split("\t")).take(3)
//
//      val ratingsRDD = rawRatings.map{
//        case Array(user,movie,rating) =>
//          Rating(user.toInt,movie.toInt,rating.toDouble)
//      }
//      println("共计:" + ratingsRDD.count(_=>true).toString()+"条ratings")
//
//      //2.创建电影ID与名称对照表
//
//      println("开始读取电影数据中...")
//      val itemRDD = sc.textFile(new File(DataDir,"u.item").toString)
//      val movieTitle = itemRDD.map(line=>line.split("\\|").take(2))
//        .map(array => (array(0).toInt,array(1))).collect().toMap
//
//
//      //3.显示数据记录数
//      val numRatings = ratingsRDD.count(_=>true)
//      val numUsers = ratingsRDD.map(_.user).distinct.count(_)
//      val numMovies = ratingsRDD.map(_.product).distinct.count(_)
//      println("共计: ratings:"+numRatings+"User "+numUsers + "Movie" +numMovies)
//
//      return (ratingsRDD,movieTitle)
//
//
//    }
//    def RecommendMovies(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserID: Int)={
//      val RecommendMovie = model.recommendProducts(inputUserID,10)
//      var i = 1
//      println("针对用户id"+inputUserID+"推荐下列电影:")
//
//      RecommendMovie.foreach{
//        r=>
//          println(i.toString() + "." + movieTitle(r.product) + "评分:"+
//          r.rating.toString)
//
//          i+=1
//      }
//
//    }
//
//    def RecommendUsers(model:MatrixFactorizationModel,movieTitle:Map[Int,String],inputMovieID:Int)={
//      val RecommendUser = model.recommendUsers(inputMovieID,10)//读取针对inputMovieID推荐前10位用户
//      var i = 1
//      println("针对电影 id" + inputMovieID + "电影名:" + movieTitle(inputMovieID.toInt)+
//      "推荐下列用户id:")
//
//      RecommendUser.foreach{
//        r=>
//          println(i.toString + "用户id:"+r.user+"评分:" + r.rating)
//          i=i+1
//      }
//    }
//}
