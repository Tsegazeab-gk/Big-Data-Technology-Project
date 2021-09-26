
package edu.cs523.hive
import org.apache.spark.sql.SparkSession

object topFollowedUsers {
    
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Hive")
      .master("local[*]")
      .config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
 
  val usersTopFollowers=sql("SELECT userId from tweets ORDER BY followersCount DESC LIMIT 5").show();
    
    
  }
}