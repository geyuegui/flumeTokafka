package com.geyuegui

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

object ConnerHelper {
  lazy val jedis = new Jedis("192.168.59.11")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.59.11:27017/recom"))
}
