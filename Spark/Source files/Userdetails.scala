package spark
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
object Userdetails {
  def parseLine(line: Array[String]) = {
    val key = line(0)
    val words = line(1).split(",")
    val pairs = words.map(friend => {
      if (key < friend) (key, friend) else (friend, key)
    })
    pairs.map(pair => (pair, words.toSet))
  }

  def parseReducer(friendlist1: Set[String], friendlist2: Set[String]) = {
    friendlist1.intersect(friendlist2)
  }

  def main(args: Array[String]): Unit = {
    //configuration
    val configuration = new SparkConf().setAppName("Mutualfriend").setMaster("local")
    val sc = new SparkContext(configuration)
    val txtdata = sc.textFile(args(0))
    val userdetails = sc.textFile(args(1))
    //formating input file containg friends to RDD
    val friendpairs = txtdata.map(line => line.split("\\t")).filter(line => line.size == 2).flatMap(parseLine).reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()
    val commonfriends = friendpairs.map(x => (x._1, x._2.size))
    //formatimg user details file to Rdd
    val userpairs = userdetails.map(line => line.split(",")).map(line => (line(0), line.slice(1,8).mkString(",")))
    val sorted = commonfriends.sortBy(_._2.toInt, false).take(10)
    val sortedrdd = sc.parallelize(sorted)
    val userrdd = sc.broadcast(userpairs.collectAsMap())
    val result1 = sortedrdd.mapPartitions({ iter =>
      val userval = userrdd.value
      for {
        ((key1, key2), values) <- iter
        if (userval.contains(key1) && userval.contains(key2))
      } yield (values+"\t"+userval.get(key1).get.replace(',','\t'), userval.get(key2).get.replace(',','\t'))
    }, preservesPartitioning = true).saveAsTextFile(args(2))
  }
}



