package com.geyuegui

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
* Created by root on 2019/9/19.
*
* 离线统计入口程序
*
* 数据流程：
* spark 读取MongoDB中数据，离线统计后，将统计结果写入MongoDB
*
* （1）评分最多电影
*
* 获取所有评分历史数据，计算评分次数，统计每个电影评分次数 --->  RateMoreMovies
*
* (2)近期热门电影
*
* 按照月统计，这个月中评分最多的电影，我们认为是热门电影，统计每个月中每个电影的评分数量   --> RateMoreRecentlyMovie
*
* (3)电影平均分
*
* 把每个电影，所有用户评分进行平均，计算出每个电影的平均评分   -->  AverageMovies
*
* (4)统计出每种类别电影Top10
*
* 将每种类别的电影中，评分最高的10个电影计算出来  --> GenresTopMovies
*
*/
object statisticApp extends App {
  val RATING="Rating"
  val MOVIE="Movie"
  var params =Map[String,Any]()
  params += "sparkCores"->"local[2]"
  params +="mongo.uri"->"mongodb://192.168.59.11:27017/recom"
  params +="mongo.db"->"recom"

  //sparkSession创建
  val sparkConfig=new SparkConf().setAppName("statisticApp").setMaster(params("sparkCores").asInstanceOf[String])
  val sparkSession =SparkSession.builder().config(sparkConfig).getOrCreate()
  //创建mongodb对象，这个对象使用隐士的方法转化
  implicit val mongoConfig =MongoConfig(params("mongo.uri").asInstanceOf[String],params("mongo.db").asInstanceOf[String])

  //加载需要用到的数据
  import sparkSession.implicits._
  val ratingsDF=sparkSession.read
    .option("uri",mongoConfig.uri)
    .option("collection",RATING)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache()
  ratingsDF.createOrReplaceTempView("ratings")

  val movieDF=sparkSession.read
    .option("uri",mongoConfig.uri)
    .option("collection",MOVIE)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache()
  //ratingsDF.createOrReplaceTempView("ratings")
  //1
  statisticAlgorithm.genreTopTen(sparkSession)(movieDF)


}
