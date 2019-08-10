/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/usr/software/spark/spark-2.4.2-bin-hadoop2.7/README.md"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val lines = sc.textFile(logFile, 2)
    val words = lines.flatMap(line=>line.split(" "))
    val ones = words.map(w=>(w,1))
    val counts = ones.reduceByKey(_ + _)
    counts.foreach(println)





    //sc.stop()
  }
}
