package com.geyuegui

import com.mongodb.casbah.commons.MongoDBObject
import kafka.Kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * 实时推荐系统
  * 1.从kafka消息队列中获取用户当前的电影评分，
  * 读取电影的相似性矩阵表，并将其设置为广播变量，然后根据相似矩阵中获取与当前电影相似的电影
  * 相似矩阵中的相似电影是历史的数据，作为所有电影的相似参考，但是每个用户的喜好可能不一样，需要根据用户最近的评分数据在这个基础上进一步的分配推荐优先级
  * 用户最近的评分数据我们这边从redis缓存中拉去。
  * 根据相似电影矩阵中获取的相似电影，用户最近的评分数据，按照公式计算每个电影的推荐优先级，将推荐的电影保存到对应用户的下
  */
object StreamingRecommender {

  val MOVIE_RECMS="MovieRecms"
  val RATINGMOVIE="Rating"
  val STREAMRESMS="StreamRecms"
  val REDISNUM=10
  val RECOMNUM=10

    //环境变量
  def main(args: Array[String]): Unit = {

   // System.gc();
    val config =Map(
      "spark.core"->"local[3]",
      "mongo.uri"->"mongodb://192.168.59.11:27017/recom",
      "mongo.db"->"recom",
      "kafka.topic"->"recom"
    )
    val sparkConf=new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.core"))
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext=sparkSession.sparkContext
    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    import sparkSession.implicits._
    /**
      * 设置电影的相似矩阵为广播
      */
    val movieRecs=sparkSession.read
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECMS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        movieRecms =>
          (movieRecms.mid,movieRecms.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()
    val movieRecsBroadCoast=sparkContext.broadcast(movieRecs)
    //广播变量需要使用聚合计算才能生效
    val a=sparkContext.makeRDD(1 to 2)
    a.map(x=>movieRecsBroadCoast.value.get(1)).count


    //获取kafka中的数据
    val ssc = new StreamingContext(sparkContext,Seconds(2))
    val kafkaParam=Map(
      "bootstrap.servers"->"192.168.59.11:9092",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id" -> "recomgroup"
    )
    val kafkaStreaming=KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaParam))

    //接收评分流  UID | MID | Score | TIMESTAMP
    //kafka数据：1|2|5.0|1564412033
    val ratingStream=kafkaStreaming.map{
      case msgs=>
        val msg= msgs.value().split("\\|")
        println("get data from kafka --- ratingStream ")
        (msg(0).toInt,msg(1).toInt,msg(2).toDouble)
    }
    //计算并更新用户的每次评分的推荐数据，将其保存到mongo
    ratingStream.foreachRDD{
      data=>
        data.map{
          case(uid,mid,score)=>
            println("get data from kafka --- ratingStreamNext ")
            //1.最近评价的电影，从redis中获取
            val userRecentMovies=getUserRecentMovie(uid:Int,REDISNUM:Int,ConnerHelper.jedis:Jedis)
            //获取d当前电影最相似的几个电影，同时要排除掉用户已经评价电影
            val topMovieSimScore=getTopMovieSimScore(uid,mid,RECOMNUM,movieRecsBroadCoast.value)
            //给最相似的电影赋优先级并
            val topMovieOrder=getTopMovieOrder(topMovieSimScore,userRecentMovies,movieRecsBroadCoast.value)
            //将数据保存到mongodb
            saveSimMovietoMongo(uid,topMovieOrder)

        }.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *
    * @param uid
    * @param num
    * @param jedis
    */
  //lpush uid:1 1129:2.0 1172:4.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
  def getUserRecentMovie(uid: Int, num: Int, jedis: Jedis) = {
    jedis.lrange("uid:"+uid,0,num).map{
      case item =>
        val datas=item.split("\\:")
        (datas(0).trim.toInt,datas(1).trim.toDouble)
    }.toArray
  }
  def log(num: Int):Double={
    math.log(num)/math.log(2)
  }

  /**
    * 获取电影mid最相似的电影，并且去掉用户已经观看的电影
    * @param uid
    * @param mid
    * @param RECOMNUM
    * @param simMovieCollections
    */
  def getTopMovieSimScore(uid: Int, mid: Int, RECOMNUM: Int, simMovieCollections: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
    //用户已经观看的电影
    val userRatingExist=ConnerHelper.mongoClient(mongoConfig.db)(RATINGMOVIE).find(MongoDBObject("uid"->uid)).toArray.map{
      item=>
        item.get("mid").toString.toInt
    }
    userRatingExist
    //电影mid所有相似的电影
    val allSimMovie=simMovieCollections.get(mid).get.toArray
    //过滤掉用户已经观看的电影，对相似电影排序，取RECOMNUM个电影
    allSimMovie.filter(x=> !userRatingExist.contains(x._1)).sortWith(_._2>_._2).take(RECOMNUM).map(x=>x._1)
  }

  /**
    *
    * @param topMovieSimScores
    * @param userRecentMovies
    * @param simMovies
    * @return
    */
  def getTopMovieOrder(topMovieSimScores: Array[Int], userRecentMovies: Array[(Int, Double)], simMovies: collection.Map[Int, Map[Int, Double]]) = {
    //存放每个待选电影的权重评分
    //这里用Array是便于使用groupBy集合计算
    val scores=ArrayBuffer[(Int,Double)]()
    //每一个待选电影的增强因子
    val incre=mutable.HashMap[Int,Int]()
    //每一个待选电影的减弱因子
    val decre=mutable.HashMap[Int,Int]()
    for (topMovieSimScore<-topMovieSimScores;userRecentMovie<-userRecentMovies){
        //相似值
      val simScore=getMoviesSimScore(simMovies,userRecentMovie._1,topMovieSimScore)
      if (simScore>0.6){
        scores +=((topMovieSimScore,simScore*userRecentMovie._2))
        if (userRecentMovie._2>3){
          incre(topMovieSimScore)=incre.getOrDefault(topMovieSimScore,0)+1
        }else{
          decre(topMovieSimScore)=decre.getOrDefault(topMovieSimScore,0)+1
        }
      }
    }
   scores.groupBy(_._1).map{
     case(mid,sim)=>
       (mid,sim.map(_._2).sum/sim.length+log(incre(mid))-log(decre(mid)))
   }.toArray
  }
  /**
    *    获取电影之间的相似度
    * @param simMovies
    * @param userRatingMovie
    * @param topSimMovie
    */
  def getMoviesSimScore(simMovies: collection.Map[Int, Map[Int, Double]],
                        userRatingMovie: Int,
                        topSimMovie: Int) = {

    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }

  }

  /**
    *
    * @param uid
    * @param topMovieOrder
    * @param mongoConfig
    */
  def saveSimMovietoMongo(uid: Int, topMovieOrder: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit= {
    val StreamCollection=ConnerHelper.mongoClient(mongoConfig.db)(STREAMRESMS)
    StreamCollection.findAndRemove(MongoDBObject("uid"->uid))
    //(Int, Double)(Int, Double)(Int, Double)(Int, Double)(Int, Double)
    //Int:Double|Int:Double|Int:Double|Int:Double|Int:Double|Int:Double
    StreamCollection.insert(MongoDBObject("uid"->uid,"recms"->topMovieOrder.map(x=>x._1+":"+x._2).mkString("|")))
    println("save to momgo success")
  }

}
