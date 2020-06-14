import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLops {

  val file = "src/main/resources/sampletweets.json"

  def main(args: Array[String]): Unit = {
    val jsonFileName = if (args.isEmpty) file else args(0)

    Logger.getLogger("org").setLevel(Level.OFF)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("-tweets-")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    //Read json file to DF
  println("Read json file to DF")
    val DF = sparkSession
      .read
      .json(jsonFileName)
      .withColumn("postedTime",
        to_utc_timestamp($"postedTime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    //Show the first 10 rows
    DF.show(10)

    //Show the scheme of DF
    println("Show the scheme of DF")
    DF.printSchema()

    // Register the DataFrame as a SQL temporary view
    DF.createOrReplaceTempView("tweetTable")

    println("Read all tweets from input files (extract the message from the tweets)\n")
    // Read all tweets from input files (extract the message from the tweets)
    val allMessages = sparkSession
      .sql("SELECT actor.displayName AS author, body AS tweet " +
        "FROM tweetTable " +
        "WHERE body IS NOT NULL ")
    allMessages.write.csv("output/tweets")

    println("Count the most active languages\n")
    // Count the most active languages
    val Langs = sparkSession
      .sql("SELECT actor.languages AS language, COUNT(*) AS count " +
        "FROM tweetTable " +
        "WHERE actor.languages IS NOT NULL " +
        "GROUP BY actor.languages " +
        "ORDER BY count DESC LIMIT 25")
    Langs.show()

    println("Get earliest and latest tweet dates\n")

    // Get latest tweet date
    val latDate = sparkSession
      .sql("SELECT actor.displayName AS author, postedTime AS latest, body AS tweet " +
        "FROM tweetTable " +
        "ORDER BY latest DESC NULLS LAST LIMIT 1")
    latDate.show()

    // Get earliest tweet date
    val eDate = sparkSession
      .sql("SELECT actor.displayName AS author, postedTime AS earliest, body AS tweet " +
        "FROM tweetTable " +
        "ORDER BY earliest ASC NULLS LAST LIMIT 1")
    eDate.show()

    // Get Top devices used among all Twitter users
    println("Get Top devices used among all Twitter users\n")
    val devs = sparkSession
      .sql("SELECT generator.displayName AS device, COUNT(*) AS count " +
        "FROM tweetTable " +
        "WHERE generator.displayName IS NOT NULL " +
        "GROUP BY generator.displayName " +
        "ORDER BY count DESC NULLS LAST LIMIT 25")
    devs.show()

    // Find how many tweets each user has
    println("Find how many tweets each user has\n")
    val HowMany = sparkSession
      .sql("SELECT actor.displayName AS author, COUNT(*) AS count " +
        "FROM tweetTable " +
        "WHERE actor.displayName IS NOT NULL " +
        "GROUP BY actor.displayName " +
        "ORDER BY count DESC")
    HowMany.show()

    // Find all the tweets by user Людмила Воронина
    println("Find all the tweets by user Людмила Воронина\n")
    val userTweets = sparkSession
      .sql("SELECT actor.displayName AS author, body AS tweet " +
        "FROM tweetTable " +
        "WHERE actor.displayName = 'Людмила Воронина' AND body IS NOT NULL")
    userTweets.show(10,false)


    // Find persons mentions for each tweet
    val userMents = sparkSession
      .sql("SELECT twitter_entities.user_mentions.name AS mentionedUsers, body " +
        "FROM tweetTable " +
        "WHERE twitter_entities.user_mentions IS NOT NULL " +
        "   AND size(twitter_entities.user_mentions) > 0 ")
    userMents.show(20)

    println("Find all the persons mentioned on tweets, order by mentions count, 10 first\n")
    val userMentions = sparkSession
      .sql("SELECT twitter_entities.user_mentions " +
        "FROM tweetTable " +
        "WHERE twitter_entities.user_mentions IS NOT NULL " +
        "   AND size(twitter_entities.user_mentions) > 0 ")
    userMentions.select(explode($"user_mentions").as("userMentions")).toDF()
    userMentions.createOrReplaceTempView("userMentions")

    sparkSession.sql("SELECT DISTINCT user_mentions.name AS name " +
      "FROM userMentions ").show(25)

    // Count how many times each person is mentioned
     println("Count how many times each person is mentioned\n")
      DF
      .select(explode(col("twitter_entities.user_mentions")) as "userMentions")
      .select("userMentions.name")
      .groupBy("name")
      .count()
      .orderBy(desc("count"))
      .show()

    // All the mentioned hashtags
    println("Find all the hashtags mentioned in a tweet\n")
    val hashTagMentions = sparkSession
      .sql("SELECT twitter_entities.hashtags " +
        "FROM tweetTable " +
        "WHERE twitter_entities.hashtags IS NOT NULL " +
        "   AND size(twitter_entities.hashtags) > 0 ")
    hashTagMentions.select(explode($"hashtags").as("hashTags")).toDF()
    hashTagMentions.createOrReplaceTempView("hashTags")
    hashTagMentions.printSchema()
    sparkSession.sql("SELECT DISTINCT hashTags.text AS hashTag, hashTags.indices as indices " +
      "FROM hashTags ").show(25)

    // Count of each hashtag being mentioned
    println("Find 10 most popular hashtags\n")
    DF
      .select(explode(col("twitter_entities.hashtags")) as "hashtag")
      .select("hashtag.text")
      .groupBy("text")
      .count()
      .orderBy(desc("count"))
      .show(10, false)
  }
}
