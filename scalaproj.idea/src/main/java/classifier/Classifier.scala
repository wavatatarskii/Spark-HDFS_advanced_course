package classifier

import com.google.gson.JsonParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Classifier {

  // Featurize Function
  def featurize(s: String): linalg.Vector = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

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
      // Extract the text
      .map(_.getAsJsonPrimitive("body").getAsString)

    // Schedule streaming tasks
    tweets.print()
    tweets
      .foreachRDD(x => {
        if (x.count() > 0) {
          // Get the features vector
          val features = x.map(featurize(_))

          // The number of clusters and iterations
          val numClusters = 10
          val numIterations = 40

          // Train KMeans model and save it to a file
          val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
          model.save(ss.sparkContext, "model/")
        }
      })

    // Start StreamingContext and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}

