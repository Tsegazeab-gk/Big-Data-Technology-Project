
package edu.cs523.hive
import org.apache.spark.sql.SparkSession

object topThreeLang {
  
  
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Hive")
      .master("local[*]")
      .config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    val top3 = sql("SELECT lang, COUNT(*) as tweet from tweets GROUP BY lang ORDER BY tweet DESC LIMIT 3").show();

  }
}