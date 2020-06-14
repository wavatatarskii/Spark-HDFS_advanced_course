import com.google.gson.{Gson, JsonParser}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_C {
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

    // Create a fake Twitter stream
    val tweets = ssc
      // Create DStream from an HDFS file
      .textFileStream("hdfs://localhost:19000/practice_5")
      // Map each string to a json object
      .map(new JsonParser().parse(_).getAsJsonObject)
      // Ignore incorrect data
      .filter(x => x.has("body"))
      // Get a JSON string
      .map(new Gson().toJson(_))

    // Schedule streaming tasks
    tweets.print()

    tweets
      .foreachRDD(x => {
        val count = x.count()
        if (count > 0) {
          // Save Tweets to an HDD
          val outputRDD = x.coalesce(1)
          outputRDD.saveAsTextFile("tweets1/")
        }
      })

    // Start StreamingContext and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
