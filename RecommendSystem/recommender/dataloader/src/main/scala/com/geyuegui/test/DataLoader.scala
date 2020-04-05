package com.geyuegui.test

import java.net.InetAddress
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


object DataLoader {

  //MongoDB 中的表 Collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  //ES_TYPE 名称
  val ES_TAG_TYPE_NAME="Movie"

  val ES_HOST_PORT_REGEX="(.+):(\\d+)".r

  def main(args: Array[String]): Unit = {
    val DATAFILE_MOVIES="E:\\bigdata\\大数据文档\\推荐系统\\reco_data\\small\\movies.csv"
    val DATAFILE_TATINGS="E:\\bigdata\\大数据文档\\推荐系统\\reco_data\\small\\ratings.csv"
    val DATAFILE_TAGS="E:\\bigdata\\大数据文档\\推荐系统\\reco_data\\small\\tags.csv"
    val params=scala.collection.mutable.Map[String,Any]()
    params+="spark.cores"->"local"
    params+="mongo.uri"->"mongodb://192.168.59.11:27017/recom"
    params+="mongo.db"->"recom"
    params += "es.httpHosts" -> "192.168.59.11:9200"
    params += "es.transportHosts" -> "192.168.59.11:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"
    //声明spark环境
    //定义mongo配置对象
    implicit val mongoConfig=new MongoConfig(params("mongo.uri").asInstanceOf[String],params("mongo.db").asInstanceOf[String])
    //定义ES配置对象
    implicit val esConfig = new ESConfig(params("es.httpHosts").asInstanceOf[String],
      params("es.transportHosts").asInstanceOf[String],
      params("es.index").asInstanceOf[String],
      params("es.cluster.name").asInstanceOf[String])
    val config=new SparkConf().setAppName("DataLoader").setMaster(params("spark.cores").asInstanceOf[String])
    val spark=SparkSession.builder().config(config).getOrCreate()
    //加载数据集
    val movieRDD=spark.sparkContext.textFile(DATAFILE_MOVIES)

    val ratingRDD=spark.sparkContext.textFile(DATAFILE_TATINGS)

    val tagRDD=spark.sparkContext.textFile(DATAFILE_TAGS)

    //将RDD转化成DDFrame
    import spark.implicits._
    //电影的DF
    val moveDF = movieRDD.map(
      line=>{
        val x = line.split("\\^")
        Movie(x(0).trim.toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9))
      }
    ).toDF()
    //评分DF
    val ratingDF=ratingRDD.map(
      line=> {
        val x=line.split(",")
        Rating(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toDouble,x(3).trim.toInt)
      }
    ).toDF()
    //ratingDF.show(3)
    val tagDF=tagRDD.map(
      line=>{
        val x=line.split(",")
        Tag(x(0).trim.toInt,x(1).trim.toInt,x(2).toString,x(3).trim.toInt)
      }
    ).toDF()
    //tagDF.show(3)

    //将数据写到mongodb
    //storeDataInmongo(moveDF,ratingDF,tagDF)
    import org.apache.spark.sql.functions._
    movieRDD.cache()
    tagDF.cache()
    //插入数据到ES

    val tagCollectionDF=tagDF.groupBy("mid").agg(concat_ws("|", collect_set("tag")).as("tags"))
    val esMoviesDF=moveDF.join(tagCollectionDF,Seq("mid","mid"),"left").select("mid","name","descri","timelong","issue","shoot","language","genres","actors","direstors","tags")
    esMoviesDF.show(30)

    storeDataInES(esMoviesDF)

    moveDF.unpersist()
    tagDF.unpersist()

    spark.close()

    //引入内置的函数库


  }
  private def storeDataInmongo(movieDF:DataFrame,ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig:MongoConfig): Unit={
    //创建mongodb的链接
    val mongoCient=MongoClient(MongoClientURI(mongoConfig.uri))
    mongoCient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoCient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()
    mongoCient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()
    //写入数据
    movieDF.write.option("uri",mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write.option("uri",mongoConfig.uri)
      .option("collection",RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write.option("uri",mongoConfig.uri)
      .option("collection",TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    mongoCient.close()
  }
  private def storeDataInES(esMovieDF:DataFrame)(implicit esConfig:ESConfig): Unit ={
    val indexName = esConfig.index

    val setting = Settings.builder().put("cluster.name",esConfig.clusterName).build()
    val esClient = new PreBuiltTransportClient(setting)

    //ESConfig 对象中transportHosts 属性保存所有的es节点信息
    esConfig.transportHosts.split(",")
      .foreach{
        case ES_HOST_PORT_REGEX(host:String,port:String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    //判断index如果存在，则删除
    if (esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists){
        esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }
    //创建index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()
    val movieOption=Map("es.nodes"->esConfig.httpHosts,
      "es.http.timeout"->"100m",
      "es.mapping.id"->"mid"
    )
    val movieTypeName=s"$indexName/$ES_TAG_TYPE_NAME"

    esMovieDF.write.options(movieOption)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)
  }

}
