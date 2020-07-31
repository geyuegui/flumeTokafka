package com.geyuegui

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

object statisticAlgorithm {

  val RATINGSCORE="ratingScore"
  val AVGSCORE="avgScore"
  val GENRETOPTEN="GenretopTen"
  val POPULARMOVIE="popularMovie"

  //计算评分最多的电影
  def scoreMost(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig):Unit={
    val ratingScoreDF =sparkSession.sql("select mid,count(1) as count from ratings group by mid order by count desc")
    ratingScoreDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",RATINGSCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  //近期热门电影
  def popularMovie(sparkSession: SparkSession)(implicit mongoConfig: MongoConfig):Unit={
    //定义日期函数
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    val changTempToDate=sparkSession.udf.register("changTempToDate",(x:Long)=>simpleDateFormat.format(new Date(x*1000L)))
    val popularMovieyDFtemp=sparkSession.sql("select uid,mid,score,changTempToDate(timestamp) as ym from ratings")
    popularMovieyDFtemp.createOrReplaceTempView("ratingtemp")
    val popularDF =sparkSession.sql("select mid,avg(score) as avgscore,ym from ratingtemp group by mid,ym order by avgscore desc")
    popularDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",POPULARMOVIE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  //统计每种类别中的前10的电影
  def genreTopTen(sparkSession: SparkSession)(movie: Dataset[Movie])(implicit mongoConfig: MongoConfig):Unit={
    //将类别转化为RDD
    val geners=List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
    val genersRDD =sparkSession.sparkContext.makeRDD(geners)

    //电影的平均分
    val averageScore =sparkSession.sql("select mid,avg(score) as averageScore from ratings group by mid").cache()
    //averageScore.createOrReplaceTempView("averageScore")
    //生成电影评分临时表
    val movieScoreTempDF=movie.join(averageScore,Seq("mid","mid")).select("mid","averageScore","genres").cache()
    import sparkSession.implicits._
    //每种电影类别前十
    val genresTopTENDF =genersRDD.cartesian(movieScoreTempDF.rdd).filter{
      case (genres,row)=>{
        row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase)
      }
    }.map{
      case(genres,row)=>{
        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("averageScore")))
      }
    }.groupByKey()
      .map {
        case(genres,items)=>{
          genreReommender(genres,items.toList.sortWith(_._2>_._2).take(10).map(x=>RecommenderItem(x._1,x._2)))
        }
      }.toDF()
    averageScore.write
      .option("uri",mongoConfig.uri)
      .option("collection",AVGSCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    genresTopTENDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRETOPTEN)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
