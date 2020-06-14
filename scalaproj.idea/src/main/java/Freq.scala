import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Freq {

//  Defining file connection strings
  val the_file = "src/main/resources/product.csv"
  val out = "output/Freq"
//  Main method
  def main(args: Array[String]): Unit ={
//  Initialize spark env.
    val conf = new SparkConf()

    conf.setAppName("W-freq")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    println(sc)
//  Reading file
    val filename = if (args.isEmpty) the_file else args(0)
//    Show the file
    println("called file" + filename + ":")
    val filerdd = sc.textFile(filename)
    //filerdd.foreach(println) ***commented bc of to much lost space in output***

    val word = filerdd.flatMap(_.split(" "))
//    let's define the pairRDD with template (key, value) for cjunter
//    to count use reduceByKey method
    val counter = word.map(x=>(x,1))
    .reduceByKey((a,b)=>(a+b))

    //Dictionary volume
    val vol = word.count().toFloat
    //Finally frequency
    val freq = counter.map(x=>(x._1,x._2,x._2.toFloat/vol))

    freq.foreach(println)

    //  I know that writing this exercise was not requested but did it just to try the skill

    val res = sc.parallelize(Seq("\n Frequencies:\n"))++ freq.map(_+ " ")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(out), true)
    res.coalesce(1,true).saveAsTextFile(out)


  }

}
