package MarketAnalysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class RetailData(sparkSession: SparkSession, input: String) {
  var dataSetSchema = StructType(Array(
    StructField("InvoiceNo", StringType, nullable = true),
    StructField("StockCode", StringType, nullable = true),
    StructField("Description", StringType, nullable = true),
    StructField("Quantity", IntegerType, nullable = true),
    StructField("InvoiceDate", StringType, nullable = true),
    StructField("UnitPrice", DoubleType, nullable = true),
    StructField("CustomerID", IntegerType, nullable = true),
    StructField("Country", StringType, nullable = true)))

  private val dataset = sparkSession.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(dataSetSchema)
    .csv(input)

}