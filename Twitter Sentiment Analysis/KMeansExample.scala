package spark
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object KMeansExample {
  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf().setAppName("KMeansExample").setMaster("local")
    val sc = new SparkContext(configuration)
    val data = sc.textFile(args(0))
    val movieDetails=sc.textFile(args(1))
    //Parsing useritemmatrix
    val parsedData = data.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()
    //Parsing moviedetails
    val movieRDD=movieDetails.map(s=>s.split("::")).map(s=>(s(0),(s(1),s(2))))
    val numClusters = 10 // Value of K in Kmeans
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    //Adding the movie id and vector of ratings
    val moviesRating = data.map(s => (s.split(' ')(0), Vectors.dense(s.split(' ').drop(1).map(_.toDouble)))).cache()
    // Print out a list of the clusters and each point of the clusters
    val clusterRating=moviesRating.map(s=>(s._1,clusters.predict(s._2)))
    //joining the movie details with its cluster information
    val joinRDD=clusterRating.join(movieRDD)
    val resultsRDD=joinRDD.map(x=>(x._2._1,(x._1,x._2._2._1,x._2._2._2))).groupByKey().mapValues(_.take(5)).saveAsTextFile(args(2))
  }
}
