package com.geyuegui

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object OfflineRecommender {
  val RATINGS="Rating"
  val MOVIES="Movie"
  val USER_RECMS="UserRecms"
  val MOVIE_REMS="MovieRecms"
  val MOVIE_RECMS_NUM=10


  def main(args: Array[String]): Unit = {
    //两个对象，一个是spark对象，用来执行代码，另外一个mongodb对象，用来保存数据
    val conf=Map(
      "spark.core"->"local[2]",
      "mongo.uri"->"mongodb://192.168.59.11:27017/recom",
      "mongo.db"->"recom"
    )

    val sparkConf=new SparkConf().setAppName("OfflineRecommender").setMaster(conf("spark.core"))
      .set("spark.executor.memory","6G")
      .set("spark.driver.memory","2G")
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig=MongoConfig(conf("mongo.uri"),conf("mongo.db"))
    import sparkSession.implicits._

    /**
      * 要生成用户的推荐矩阵，需要评分数据，从评分数据中获取用户信息和评分信息，从电影collection中获取电影信息
      */
    //获取评分
    val ratingRDD=sparkSession.read
      .option("uri",mongoConfig.uri)
      .option("collection",RATINGS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating=>(rating.uid,rating.mid,rating.score)).cache()
    //电影
    val movieRDD=sparkSession.read
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIES)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //生成用户，电影对象集合
    val users=ratingRDD.map(_._1).distinct().cache()
    val userMovies=users.cartesian(movieRDD)
    /**
      * 构建模型，需要的数据transdata,特征值个数，迭代次数，迭代步长
      */

      val (range,itrators,lambda)=(50,5,0.01)

    val transdatas= ratingRDD.map(x=>Rating(x._1,x._2,x._3))
    val models =ALS.train(transdatas,range,itrators,lambda)
    //推荐矩阵
    val preRatings=models.predict(userMovies)
    //获取用户推荐电影，并将其转化为mogodb存储模式
    val userRecmsDF=preRatings.filter(_.rating>0)
      .map(x=>(x.user,(x.product,x.rating)))
      .groupByKey()
      .map{
        case (uid,recs)
        =>UserRecs(uid,recs.toList.sortWith(_._2>_._2).take(MOVIE_RECMS_NUM).map(x=>RecommenderItem(x._1,x._2)))}
      .toDF()
    //存储到mongodb上
    userRecmsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECMS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /**
      * 通过模型计算电影的相似矩阵
      */
    //得到电影的特征矩阵，然后通过余弦相似性计算电影之间的相似性
    val movieFeatures=models.productFeatures.map{
      case(mid,feature)
      =>(mid,new DoubleMatrix(feature))
    }


    val movieRecmsDF=movieFeatures.cartesian(movieFeatures)
      .filter{
        case(a,b)=>a._1!=b._1
      }.map{
        case(a,b)=>(a._1,(b._1,this.consimScore(a._2,b._2)))//(Int,(Int,Double))
      }.filter(_._2._2>0.6)
      .groupByKey()
      .map{
        case(mid,consmids)=>
          MovieRecs(mid,consmids.toList.map(x=>RecommenderItem(x._1,x._2)))
      }.toDF()
    movieRecmsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIE_REMS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.close()

  }
  def consimScore(feature1: DoubleMatrix, feature2: DoubleMatrix): Double = {
      (feature1.dot(feature2))/(feature1.norm2()*feature2.norm2())
  }

}
