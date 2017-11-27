package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Yelpstats {
  def main(args: Array[String]): Unit = {
    //configuration
    val configuration=new SparkConf().setAppName("Yelpstats").setMaster("local")
    val sc=new SparkContext(configuration)
    //taking input file
    val businessdata=sc.textFile(args(0))
    val reviewdata=sc.textFile(args(1))
    //creating RDD from text files
    val businessrdd=businessdata.map(line=>line.split("::")).map(line=>(line(0),(line(1),line(2))))
    val reviewrdd=reviewdata.map(line=>line.split("::")).map(line=>(line(2),1)).reduceByKey(_+_)
    //TAking the top ten mostly rated business
    val results=businessrdd.join(reviewrdd).reduceByKey((x,y)=>x).sortBy(_._2._2.toInt,false).take(10)
    val resultsrdd=sc.parallelize(results).map(x=>x._1+"\t"+x._2._1._1+"\t"+x._2._1._2+"\t"+x._2._2).saveAsTextFile(args(2))
  }

}
