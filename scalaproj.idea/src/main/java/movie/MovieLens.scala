package movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieLens {

  val directory = "src/main/resources/"
  val ratings = "ratings.csv"
  val movie = "movies.csv"
  val persRating = "personalRatings.csv"

//  defining the data frame format for datasets
  def data(ss: SparkSession, schemaName: String, dataname: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      //      .option("inferSchema", "true")
      .schema(Scheme(schemaName))
      .csv(directory + dataname)
      .toDF()
  }
// defining data schemes
  def Scheme(name: String): StructType = {
    if ("ratings".equals(name) || "personalRatings".equals(name))
      return StructType(Array(
        StructField("u_id", IntegerType, nullable = true),
        StructField("m_id", IntegerType, nullable = true),
        StructField("rating", IntegerType, nullable = true),
        StructField("time", IntegerType, nullable = true)))
    if ("movies".equals(name))
      return StructType(Array(
        StructField("movie_id", IntegerType, nullable = true),
        StructField("movie_name", StringType, nullable = true),
        StructField("genre", StringType, nullable = true)))
    null
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext("local[2]", "MovieLens")

    val ss = SparkSession
      .builder()
      .appName("movieieLens")
      .master("local[*]")
      .getOrCreate()

    val rating = data(ss, "ratings", ratings)
    rating.show(10,false)
    rating.printSchema()

    val movies = data(ss, "movies", movie)
    movies.show(10,false)
    movies.printSchema()

    val personalRatings = data(ss, "personalRatings", persRating)
    personalRatings.show(10,false)
    personalRatings.printSchema()

    val myRating = rating.union(personalRatings)
    myRating.show(10,false)

    // Split dataset into training and testing parts
    val Array(training, test) = myRating.randomSplit(Array(0.5, 0.5), seed = 11L)

    println("training")
    training.show(10)
    println("test")
    test.show(10)
    //Initialize Alternate Least Squares Matrix Factorization model
    val als = new ALS()
      .setMaxIter(3)
      .setRegParam(0.01)
      .setUserCol("u_id")
      .setItemCol("m_id")
      .setRatingCol("rating")

    //Get trained model
    val model = als.fit(training)

    val predictions = model.transform(test).na.drop

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    println(s"\nRMSE = $rmse")

    val ratingWithMyRats = rating.union(myRating)

    //Get My Predictions
    val myPredictions = model.transform(myRating).na.drop

    myPredictions.show(10)
  }
}
