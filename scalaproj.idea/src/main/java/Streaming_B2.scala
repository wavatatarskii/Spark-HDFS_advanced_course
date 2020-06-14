import com.google.gson.JsonParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_B2 {
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

    // Create a hashtags stream
    val hashtags = tweets
      // Extract the body
      .map(_.getAsJsonPrimitive("body").getAsString)
      // Map the body to words
      .flatMap(_.split(" "))
      // Choose only non-empty words
      .filter(_.length > 0)
      // Choose only hashtags
      .filter(_.charAt(0) == '#')

    // Schedule streaming tasks
    hashtags
      .foreachRDD(x => {
        println(x
          // Get descending frequencies
          .map(y => (y, 1))
          .reduceByKey((y, z) => y + z)
          .sortBy(y => y._2, ascending = false)
          .map(y => s"${y._1}\t[${y._2}]")
          .collect
          .mkString("\n")
        )
      })

    println("\n TOP 10 HASHTAGS")

    hashtags
      .foreachRDD(x => {
        println(x
          // Get descending frequencies
          .map(y => (y, 1))
          .reduceByKey((y, z) => y + z)
          .sortBy(y => y._2, ascending = false)
          .map(y => s"${y._1}\t[${y._2}]")
          .collect
          // Get Top 10
          .take(10)
          .mkString("\n")
        )
      })
    // Start StreamingContext and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
