
package edu.cs523

import java.util.Properties
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object SparkConsumer {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TsegazeabApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    
    Logger.getRootLogger().setLevel(Level.ERROR) 
    
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("topic-tweets").toSet
    
    val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    .map(twt=>twt.split("\t"))
    .map(splt=>(splt(0).toLong,splt(1),splt(2).toInt,splt(3),splt(4).toInt,splt(5),splt(6).toInt,
        splt(7).toBoolean,
        splt(8)))
    tweets.foreachRDD((rdd,time)=>{
         
      val spark = SparkSession.builder().appName("Spark Hive").master("local[*]")
      .config("hive.metastore.warehouse.uris","thrift://localhost:9083") 
      .enableHiveSupport() .getOrCreate() //create a new context or recover from checkpoint 
 
      import spark.implicits._
      import spark.sql
      
      val tweetrepartitioned = rdd.repartition(1).cache()
      val tweetDataFrame = tweetrepartitioned.map(twt => Record(twt._1, twt._2,twt._3, twt._4,twt._5, twt._6,twt._7, twt._8,twt._9
          )).toDF()
      val sqlContext1 = new HiveContext(spark.sparkContext)
      import sqlContext1.implicits._
      
      tweetDataFrame.write.mode(SaveMode.Append).saveAsTable("tweets")
     
      tweetDataFrame.createOrReplaceTempView("tweetable")
      val tables = spark.sqlContext.sql("select * from tweetable")
      println(s"========= $time =========")
      tables.show()
    })
    
    /*  
         setting a checkpoint to allow RDD recovery
     */  
    ssc.checkpoint("checkpoint_TwitterApp") // set checkpoint directory

    ssc.start()
    ssc.awaitTermination()
  }  
}

                     
case class Record(
    userId: Long,   //User Id
    lang:String,       //language
    friendsCount:Int,     // Friends count
    Location:String,      //location
    followersCount:Int,    //number of followers
    deviceUsed:String,    
    retweetCount:Int,    // count
    isRetweeted:Boolean,      // is retweeted
    text:String    // post body
    )
    
    
    
