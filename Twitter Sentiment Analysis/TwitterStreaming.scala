package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterStreaming {
def main(args: Array[String]): Unit = {
    //setting up the configuration
    val conf = new SparkConf().setAppName("Twitterstream").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val filters=Seq(args(0))
    //sparkstreaming context
    val ssc = new StreamingContext(sc, Seconds(5))
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret","")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")
    val stream = TwitterUtils.createStream(ssc, None,filters)
    //filtering based on tags,location,language
    val hashtweets=stream.filter(_.getLang()=="en").filter(
      status => Option(status.getPlace()) match {
        case Some(_) => true
        case None => false
      })
    //sentiment analysis
   val data=hashtweets.map{status=>
      val sentiment: String =SentimentAnalysis.detectSentiment(status.getText)
     val tags="#"+args(0)
      val location=status.getPlace.getCountry()
      (tags,status.getText(),sentiment,location)
    }
    data.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
