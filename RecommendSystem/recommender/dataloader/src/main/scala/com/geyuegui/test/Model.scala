package com.geyuegui.test

/*
电影数据信息
 */
case class Movie(val mid:Int,val name:String,val descri:String,
                 val timelong:String,val issue:String,val shoot:String,
                 val language:String,val genres:String,val actors:String,val direstors:String)

/*
电影评分信息
 */
case class Rating(val uid:Int,val mid:Int,val score:Double,val timestamp:Int)

/*
电影标签信息
 */
case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)

case class MongoConfig(val uri:String,val db:String)

/**
  * ES配置对象
  */
case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)

