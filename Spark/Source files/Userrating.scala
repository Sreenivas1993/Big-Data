package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
object Userrating {
  def main(args: Array[String]): Unit = {
    //configuration
    val configuration=new SparkConf().setAppName("Userrating").setMaster("local")
    val sc=new SparkContext(configuration)
    //Creating RDD files
    val businessdata=sc.textFile(args(0))
    val reviewdata=sc.textFile(args(1))
    //filtering RDD to only contain Palo Alto business
    val businessrdd=businessdata.map(line=>line.split("::")).filter(line=>line(1).contains("Palo Alto")).map(line=>(line(0),line(1)))
    val businessval=sc.broadcast(businessrdd.collectAsMap())
    //broadcasting business RDD and joining with user details
    val reviewrdd=reviewdata.map(line=>line.split("::")).map(line=>(line(1),line(2),line(3))).mapPartitions({ line =>
      val bval = businessval.value
      for {
        (key1, key2, value) <- line
        if (bval.contains(key2))
      } yield (key1,key2,value)
    },preservesPartitioning = true).map(value=>(value._1,(value._3.toFloat,1)))
    //Taking the average rating of user
    reviewrdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues{case (sum,count)=>sum/count}.map(values=>values._1+"\t"+values._2).saveAsTextFile(args(2))
  }
}

