package edu.cs523.hive
import org.apache.spark.sql.SparkSession

object TwenyTweets {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Hive")
      .master("local[*]")
      .config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    
    //query to get 20 tweets
    val d = sql("SELECT * from tweets limit 20").show(); 
     }
}