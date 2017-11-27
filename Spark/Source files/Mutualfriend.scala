package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Mutualfriend {
  //function for parsing the input and forming map key value pair
  def parseLine(line: Array[String]) = {
    val key = line(0)
    val words = line(1).split(",")
    val pairs = words.map(friend => {
      if (key < friend) (key, friend) else (friend, key)
    })
    pairs.map(pair => (pair, words.toSet))
  }
  //function for reducer
  def parseReducer(friendlist1: Set[String], friendlist2: Set[String]) = {
    friendlist1.intersect(friendlist2)
  }

  def main(args: Array[String]): Unit = {
    //configuration for spark session
    val configuration = new SparkConf().setAppName("Mutualfriend").setMaster("local")
    val sc = new SparkContext(configuration)
    val txtdata = sc.textFile(args(0))
    //RDD performing map and reduce operation
    val friendpairs = txtdata.map(line => line.split("\\t")).filter(line => line.size == 2).flatMap(parseLine).reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()
    //results saving to text file
    val results = friendpairs.map(x => (x._1, x._2.size)).map(x=>x._1.toString()+"\t"+x._2.toString()).saveAsTextFile(args(1))
  }
}
