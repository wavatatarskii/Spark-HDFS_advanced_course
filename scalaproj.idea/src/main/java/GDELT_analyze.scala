
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

//import scala.reflect.internal.util.TableDef.Column

import utilities.GDELTdata



object GDELT_analyze {

  def main(args: Array[String]): Unit = {

    // Suppress unessesary log output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val csv = "src/main/resources/gdelt.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-gdelt-analysis")
      .master("local[*]")
      .getOrCreate()


    val gdelt = new GDELTdata(sparkSession,csv)

    // All events in USA or RUSSIA
    gdelt.usa_or_rus()
    gdelt.most_mentioned()
    gdelt.join_desc()
    gdelt.get_top10_events()

  }
}