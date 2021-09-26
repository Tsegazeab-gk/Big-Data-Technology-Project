
package edu.cs523

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.{Level, Logger}

/** 
 *  Listens to stream of tweet and produces to Kafka
 *  
 **/

object ProduceTweets {
 

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TsegazeabApp")
    .setMaster("local[2]")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))    //stream every 3 seconds
    
    Logger.getRootLogger().setLevel(Level.ERROR)
    
    // Twitter API integration tokens
    val key = "wq41b14nM6wE9UdLYGcV9C81I"  
    val secret = "DWpdpJslCjqY4lqvVP1pk9MwId2ebSQ6IValPdud9yKxTDu38v"  
    val token ="744257963741720576-0iRSGfNMKEqWbDqHBdc3WZeKTJgdAxM"  
    val tokenSecret = "YE3P0CyV3ucOFKb5sbBPsVcfTNVwFo10oDqm8fTqsiK5W"

    //connecting to Twitter config
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true)
    .setOAuthConsumerKey(key)
    .setOAuthConsumerSecret(secret)
    .setOAuthAccessToken(token)
    .setOAuthAccessTokenSecret(tokenSecret)

    val auth = new OAuthAuthorization(cb.build)
    val tweetsStream = TwitterUtils.createStream(ssc, Some(auth))

    val tweets = tweetsStream.map(
        tweets => (
        tweets.getUser.getId, 
        tweets.getLang,
        tweets.getUser.getFriendsCount,
        tweets.getUser.getLocation,
        tweets.getUser.getFollowersCount,
        tweets.getSource.split("[<,>]")(2),
        tweets.getRetweetCount
       ,tweets.isRetweeted
    ,tweets.getText))
    .map(_.productIterator.mkString("\t"))
    
    tweets.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionIter =>
        val props = new Properties()
        val bootstrap = "localhost:9092" 
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", bootstrap)
        
        //creating kafka producer object
        val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          
          // topic-tweets
          val data = new ProducerRecord[String, String]("topic-tweets", null, dat)  
          producer.send(data)
        }
        producer.flush()
        producer.close()
        
        
        
      }
    }
    
  // start the streaming computation
    ssc.start()
    
    ssc.awaitTermination()
  }  
}
