package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object Yelpstatsdf {
  def main(args: Array[String]): Unit = {
    //configuration
    val sc=SparkSession.builder().master("local").appName("Yelpstatsdf").getOrCreate()
    //creating RDD from input files
    val businessdata=sc.sparkContext.textFile(args(0))
    val reviewdata=sc.sparkContext.textFile(args(1))
    //creating dataframe from RDD
    val businessrdd=businessdata.map(line=>line.split("::")).map(line=>(line(0),line(1),line(2))).map(attributes=>Row(attributes._1,attributes._2,attributes._3))
    val businessschema=new StructType().add(StructField("businessid",StringType,true)).add(StructField("address",StringType,true)).add(StructField("categories",StringType,true))
    val businessdf=sc.createDataFrame(businessrdd,businessschema)
    val reviewrdd=reviewdata.map(line=>line.split("::")).map(line=>(line(2))).map(attributes=>Row(attributes))
    val reviewSchema=new StructType().add(StructField("businessid",StringType,true))
    val reviewdf=sc.createDataFrame(reviewrdd,reviewSchema)
    businessdf.createOrReplaceTempView("business")
    reviewdf.createOrReplaceTempView("review")
    //sql
    import sc.sqlContext.implicits._
    val reviewcount=sc.sql("SELECT businessid,count(businessid) as ratedcount from review GROUP BY businessid").sort($"ratedcount".desc).limit(10)
    reviewcount.createOrReplaceTempView("reviewcount")
    val results=sc.sql("SELECT DISTINCT b.businessid,b.address,b.categories,r.ratedcount from business b inner join reviewcount r where b.businessid=r.businessid").sort($"ratedcount".desc).collect()
    val resultsRDD=sc.sparkContext.parallelize(results).map(x=>x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).saveAsTextFile(args(2))
  }

}
