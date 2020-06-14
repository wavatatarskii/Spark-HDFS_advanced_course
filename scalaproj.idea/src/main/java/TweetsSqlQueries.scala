/**
  * Created by ALINA on 27.09.2017.
  */

import org.apache.spark.sql.SparkSession

object TweetsSqlQueries {

  def main(args: Array[String]): Unit = {
    val jsonFile = if (args.isEmpty) "src/main/resources/sampletweets.json" else args(0);

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
    tweetsDF.show(100);

    //Show thw scheme of DF
    tweetsDF.printSchema();

    // Register the DataFrame as a SQL temporary view
    tweetsDF.createOrReplaceTempView("tweetTable")

    //Get the actor name and body of messages in Russian
    sparkSession.sql(
      " SELECT actor.displayName, object.twitter_lang, body" +
        " FROM tweetTable WHERE body IS NOT NULL" +
        " AND object.twitter_lang = 'ru'")
      .show(100)

    //Get the most popular languages
    sparkSession.sql(
      " SELECT actor.languages, COUNT(*) as cnt" +
        " FROM tweetTable " +
        " GROUP BY actor.languages ORDER BY cnt DESC LIMIT 25")
      .show(100)

    //Top devices used among all Twitter users
    sparkSession.sql(
      " SELECT generator.displayName, COUNT(*) cnt" +
        " FROM tweetTable " +
        " WHERE  generator.displayName IS NOT NULL" +
        " GROUP BY generator.displayName ORDER BY cnt DESC LIMIT 25")
      .show(100)

    //Top users who has a lot of friends
    sparkSession.sql(
      " SELECT actor.displayName, actor.friendsCount" +
        " FROM tweetTable " +
        " ORDER BY actor.friendsCount DESC LIMIT 25")
      .show(100)
  }
}
