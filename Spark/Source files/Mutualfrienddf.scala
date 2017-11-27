package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object Mutualfrienddf {
  //creating map output
  def parseLine(line: Array[String]) = {
    val key = line(0)
    val words = line(1).split(",")
    val pairs = words.map(friend => {
      if (key < friend) (key, friend) else (friend, key)
    })
    pairs.map(pair => (pair, words.toSet))
  }
  //creting reducer output
  def parseReducer(friendlist1: Set[String], friendlist2: Set[String]) = {
    friendlist1.intersect(friendlist2)
  }
  def main(args: Array[String]): Unit = {
    //sparksession
    val sc =SparkSession.builder.master("local").appName("Mutualfrienddf").getOrCreate()
    //for rdd to datframe conversion
    import sc.implicits._
    val txtdata=sc.sparkContext.textFile(args(0))
    //formatting input file
    val friendpairs = txtdata.map(line => line.split("\\t")).filter(line => line.size == 2).flatMap(parseLine).reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()
    val friendRDD=friendpairs.map(x=>(x._1._1,x._1._2,x._2.toList)).map(attributes=>Row(attributes._1,attributes._2,attributes._3))
    //schema for dataframe
    val schema=new StructType().add(StructField("friend1",StringType,true)).add(StructField("friend2",StringType,true)).add(StructField("mutual",ArrayType(StringType),true))
    //creating datframe file
    val frienddf=sc.createDataFrame(friendRDD,schema)
    //sql
    frienddf.createOrReplaceTempView("friends")
    val results=sc.sql("SELECT friend1,friend2,size(mutual) as friendscount FROM friends").collect()
    val resultsRDD=sc.sparkContext.parallelize(results).map(x=>x(0)+","+x(1)+"\t"+x(2))
    resultsRDD.saveAsTextFile(args(1))
  }

}
