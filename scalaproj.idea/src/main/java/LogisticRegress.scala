import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object LogisticRegressionDF {

  val file = "src/main/resources/diabetes.csv"
  var diabets = StructType(Array(
    StructField("pregnancy", IntegerType, nullable = true),
    StructField("glucose", IntegerType, nullable = true),
    StructField("arterial pressure", IntegerType, nullable = true),
    StructField("thickness of TC", IntegerType, nullable = true),
    StructField("insulin", IntegerType, nullable = true),
    StructField("body mass index", DoubleType, nullable = true),
    StructField("heredity", DoubleType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("diabetes", IntegerType, nullable = true)))

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val diabetesFile = if (args.isEmpty) file else args(0)

    val sparkSession = SparkSession
      .builder()
      .appName("logistic regression with data frames")
      .master("local[*]")
      .getOrCreate()

    //  load the csv file with predefined structure
    val diabetes = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(diabets)
      .csv(diabetesFile)
    diabetes.show(10)

    val Assemb = new VectorAssembler().
      setInputCols(Array(
        "pregnancy", "glucose", "arterial pressure",
        "thickness of TC", "insulin", "body mass index",
        "heredity", "age")).
      setOutputCol("features")

    val features = Assemb.transform(diabetes)
    features.show(10,false)

    val Transform = new StringIndexer().setInputCol("diabetes").setOutputCol("label")
    val labeled = Transform.fit(features).transform(features)
    labeled.show(10,false)

    // Split data into training (60%) and test (40%)
    val Splits = labeled.randomSplit(Array(0.7, 0.3), seed = 11L) // acc ~ 83.4%
//  val Splits = labeled.randomSplit(Array(0.6, 0.4), seed = 11L) //acc ~ 70%
  val Splits = labeled.randomSplit(Array(0.8, 0.2), seed = 11L) //acc ~ 82%

    val train = Splits(0)
    val test = Splits(1)

    val logReg = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.5)

    // Train the model
    val model = logReg.fit(train)

    // Make predictions on test data
    val pred = model.transform(test)
    pred.show(10,false)

    // Evaluate the precision and recall
    val truecount = pred.where("label == prediction").count()
    val totalcount= pred.count()

    val evaluate = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val accuracy = evaluate.evaluate(pred)

    println(s"Accuracy="+accuracy)

    val PredLabel = pred
      .select("rawPrediction", "label")
      .rdd
      .map(x => (x(0).asInstanceOf[DenseVector](1), x(1).asInstanceOf[Double]))

    val classif = new BinaryClassificationMetrics(PredLabel)

    println("\nArea under the precision-recall curve: " + classif.areaUnderPR())
    println("Area under the receiver operating characteristic (ROC) curve: " + classif.areaUnderROC())

  }
}