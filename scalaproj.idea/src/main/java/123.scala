import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    println(sc) }
}