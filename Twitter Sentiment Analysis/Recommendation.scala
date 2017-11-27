package spark
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Recommendation {
  def main(args: Array[String]): Unit = {
    val configuration = new SparkConf().setAppName("Recommendation").setMaster("local")
    val sc = new SparkContext(configuration)
    val ratingData=sc.textFile(args(0))
    val Array(trainingData, testData) = ratingData.randomSplit(Array(0.6, 0.4), seed = 12345)
    val trainingRDD=trainingData.map(_.split("::") match { case Array(user, item, rate,timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)})
    val testRDD=testData.map(_.split("::") match { case Array(user, item, rate,timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)})
    //build the reccomendation using ALS
    val rank=10
    val numIterations=10
    val model=ALS.train(trainingRDD,rank,numIterations,0.01)
    //Evaluate model on rating data
    val usersProducts = testRDD.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = testRDD.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    val accuracy=ratesAndPreds.map{case((user,product),(r1,r2)) =>
      var count=0
      if(Math.abs(r1-r2) <= 1){
        count = 1
      }
      count
    }.mean()
    println("Accuracy = " + accuracy)
  }
}
