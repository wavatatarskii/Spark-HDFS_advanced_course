import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RddNumOps {
// Defining file connection strings, global variables and scalars
  val the_filename = "src/main/resources/input.txt"
  val num_count = 20
  val out_dir = "hdfs://localhost:19000/output/output.txt"
//  Main method
  def main(args: Array[String]): Unit = {
// Initialize spark env.
    val conf = new SparkConf()

    conf.setAppName("Rdd-Num-Ops")
    conf.setMaster("local[2]")


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sc = new SparkContext(conf)
    println(sc)
// Reading the file
    val filename = if (args.isEmpty) the_filename else args(0)

    println("called file" + filename + ":")
    val filerdd = sc.textFile(filename)
    filerdd.foreach(println)
// We initialize variables for numbers mapping
    val rows = filerdd.map(_.split(" "))
      /*Create the Resilient Distributed Dataset of numbers */
    val nums = rows.map(_.map(_.toInt))

//    println("\nRDD[Array[Int]]/")
//    println("Array of nums")
//    nums.map(_.mkString("", " ","\n")).takeOrdered(num_count).foreach(print)

//   ********************* Defining methods for RDD[Array[Int]]*********************************

//    println("\nSum of the numbers from each row:")
    val Sumrows = nums.map(_.sum.toString)
    Sumrows.takeOrdered(num_count).foreach(println)

//    println("\nSum of the numbers divisible by 5 from each row:")
    val MultyFive =nums.map(_.filter(_ % 5 == 0).sum.toString)
    MultyFive.foreach(println)

//    println("\nMaximum of the numbers from each row:")
    val MaxR = nums.map(_.max.toString)
    MaxR.foreach(println)

//    println("\nMinimum of the numbers from each row:")
    val MinR = nums.map(_.min.toString)
    MinR.foreach(println)

//    println("\nDistinct set of all numbers:")
    val SetDist = nums.map(_.distinct)
    SetDist.map(_.mkString("", " ","\n")).takeOrdered(num_count).foreach(print)


//   ********************* Defining methods for RDD[Int]*********************************

//    println("\n\nRDD[Int]")
//    println("Get array of all the nums")

//    Define the tuple for RDD[Int] in flatmap
    val flatinteger = filerdd.flatMap(_.split(" ")).map(_.toInt)
//    flatinteger.map(_ + " ").foreach(println)

    println("\nSum of the numbers from each row:")
    val flatSumrows = flatinteger.reduce(_+_).toString
//    println(flatSumrows)

    println("\nSum of the numbers divisible by 5 from each row:")
    val flatMultyFive = flatinteger.filter(_%5==0).reduce(_ + _).toString
    println(flatMultyFive)

//    println("\nMaximum of the numbers from each row:")
    val flatMaxR = flatinteger.max.toString
//    println(flatMaxR)

//    println("\nMinimum of the numbers from each row:")
    val flatMinR = flatinteger.min.toString
//    println(flatMinR)

    //println("\nDistinct  of all numbers:")
    val flatSetDist = flatinteger.distinct()
    //flatSetDist.takeOrdered(num_count).mkString("", " ","\n").foreach(print)


//*************************Save all the operations outputs together****************************
    val res =
      sc.parallelize(Seq("RDD[Array[Int]]"))++
      sc.parallelize(Seq("\nGet array of all the numbers:\n"))++ filerdd ++
        sc.parallelize(Seq("\nSum of the numbers from each row:\n"))++ Sumrows ++
        sc.parallelize(Seq("\nSum of the numbers divisible by 5 from each row:\n")) ++ MultyFive ++
    sc.parallelize(Seq("\nMaximum of the numbers from each row:\n"))++ MaxR ++
        sc.parallelize(Seq("\nMinimum of the numbers from each row:\n"))++ MinR ++
        sc.parallelize(Seq("\nDistinct set of all numbers:\n"))++ SetDist.map(_.mkString("", " ", ""))++
    sc.parallelize(Seq("\nRDD[Int]"))++
    sc.parallelize(Seq("\nGet array of all the numbers:\n"))++ flatinteger.map(_+ " ") ++
        sc.parallelize(Seq("\nSum of the numbers from each row:\n" + flatSumrows)) ++
        sc.parallelize(Seq("\nSum of the numbers divisible by 5 from each row:\n" + flatMultyFive)) ++
        sc.parallelize(Seq("\nMaximum of the numbers from each row:\n"+flatMaxR)) ++
        sc.parallelize(Seq("\nMinimum of the numbers from each row:\n" + flatMinR)) ++
        sc.parallelize(Seq("\nDistinct set of all numbers:\n")) ++ flatSetDist.map(_+ " ")
// Using HDFS to save results in text file
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(out_dir), true)
    res.coalesce(1,true).saveAsTextFile(out_dir)


  }


}
