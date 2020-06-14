import com.google.gson.JsonParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingBone {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a configuration
    val conf = new SparkConf()
    conf.setAppName("TaskB_1")
    conf.setMaster("local[2]")

    // Initialize SparkSession
    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // Initialize SparkContext
    val ssc = new StreamingContext(ss.sparkContext, Seconds(10))

    // Create fake Twitter Stream
    val tweets = ssc
      // Create DStream from an HDFS file
      .textFileStream("hdfs://localhost:19000/practice_5")
      // Map each string to a json object
      .map(new JsonParser().parse(_).getAsJsonObject)
      // Ignore incorrect data
      .filter(x => x.has("body"))

    // Schedule streaming tasks
    tweets
      // Extract the body
      .map(_.getAsJsonPrimitive("body").getAsString)
      // Print the body
      .print()

    // Start StreamingContext and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
