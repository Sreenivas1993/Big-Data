package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType,StructField,StructType}
object Userratingdf {
  def main(args: Array[String]): Unit = {
    //Configuration
    val sc=SparkSession.builder().master("local").appName("Userratingdf").getOrCreate()
    //taking input files
    val businessdata=sc.sparkContext.textFile(args(0))
    val reviewdata=sc.sparkContext.textFile(args(1))
    //constructing dataframe from business rdd
    val businessRDD=businessdata.map(line=>line.split("::")).map(line=>(line(0),line(1))).map(attributes=>Row(attributes._1,attributes._2))
    val businessSchema=new StructType().add(StructField("businessid",StringType,true)).add(StructField("address",StringType,true))
    //constructing dataframe from review rdd
    val reviewRDD=reviewdata.map(line=>line.split("::")).map(line=>(line(1),line(2),line(3))).map(attributes=>Row(attributes._1,attributes._2,attributes._3))
    val reviewSchema=new StructType().add(StructField("userid",StringType,true)).add(StructField("businessid",StringType,true)).add(StructField("rating",StringType,true))
    val businessdf=sc.createDataFrame(businessRDD,businessSchema)
    val reviewdf=sc.createDataFrame(reviewRDD,reviewSchema)
    businessdf.createOrReplaceTempView("business")
    reviewdf.createOrReplaceTempView("review")
    val results1=sc.sql("SELECT businessid,address FROM business WHERE address LIKE '%Palo Alto%'")
    results1.createOrReplaceTempView("result")
    val results=sc.sql("SELECT r.userid,AVG(r.rating) FROM result re inner join review r WHERE r.businessid=re.businessid GROUP BY r.userid").collect()
    val resultsRDD=sc.sparkContext.parallelize(results).saveAsTextFile(args(2))
  }

}
