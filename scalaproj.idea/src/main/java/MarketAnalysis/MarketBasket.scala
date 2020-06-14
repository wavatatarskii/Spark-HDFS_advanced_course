package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object MarketBasket {

  val file = "src/main/resources/Online Retail.csv"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = if (args.isEmpty) file else args(0)
    val sc = new SparkContext("local[2]", "mlLib")

    val sess = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    val Schema = StructType(Array(
      StructField("InvoiceNo", StringType, nullable = true),
      StructField("StockCode", StringType, nullable = true),
      StructField("Description", StringType, nullable = true),
      StructField("Quantity", IntegerType, nullable = true),
      StructField("InvoiceDate", StringType, nullable = true),
      StructField("UnitPrice", DoubleType, nullable = true),
      StructField("CustomerID", IntegerType, nullable = true),
      StructField("Country", StringType, nullable = true)))

    val data = sess.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(Schema)
      .csv(input)

    data.show(20,false)

    //read csv-file with purchases into RDD
    val csv_to_rdd: RDD[String] = sc.textFile(input)

    //get transactions
    val productsRDD: RDD[Array[String]] = csv_to_rdd.map(s => s.split(";"))


    data.createOrReplaceTempView("dataTable")

//    get Invoice and StockCode
    val GetInvoicesAndStockCodes = sess.sql(
      " SELECT InvoiceNo, StockCode" +
        " FROM dataTable" +
        " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
    GetInvoicesAndStockCodes.show(20,false)

    val transactions = GetInvoicesAndStockCodes.rdd.map(row => (row(0), row(1).toString)).groupByKey().map(x => x._2.toArray.distinct)

//    get frequency patterns
    val fpg = new FPGrowth()
      .setMinSupport(0.02)
    val model = fpg.run(transactions)


    //get association patterns
    val minConf = 0.01
    val patterns = model.generateAssociationRules(minConf).sortBy(r => r.confidence, ascending = false)

//    Get Descriptions
    val GetDescriptionsAndStckCodes = sess.sql(
      "SELECT DISTINCT StockCode, Description" +
        " FROM dataTable" +
        " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
    GetDescriptionsAndStckCodes.show(30,false)
    val dictionary = GetDescriptionsAndStckCodes.rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    patterns.collect().foreach { pattern =>
      println(
        pattern.antecedent.map(s => dictionary(s)).mkString("[", ",", "]")
          + " => " + pattern.consequent.map(s => dictionary(s)).mkString("[", ",", "]")
          + ", " + pattern.confidence)
    }

  }
}
