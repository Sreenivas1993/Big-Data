package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType,StringType, StructField, StructType}
object Userdetailsdf {
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
    //SparkSession
    val sc =SparkSession.builder.master("local").appName("Userdetailsdf").getOrCreate()
    val txtdata=sc.sparkContext.textFile(args(0))
    val userdata=sc.sparkContext.textFile(args(1))
    //formating input file to map (key,value) pairs
    val friendpairs = txtdata.map(line => line.split("\\t")).filter(line => line.size == 2).flatMap(parseLine).reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()
    val friendRDD=friendpairs.map(x=>(x._1._1,x._1._2,x._2.toList)).map(attributes=>Row(attributes._1,attributes._2,attributes._3))
    //creating schema
    val friendSchema=new StructType().add(StructField("friend1",StringType,true)).add(StructField("friend2",StringType,true)).add(StructField("mutual",ArrayType(StringType),true))
    //creating dataframe
    val frienddf=sc.createDataFrame(friendRDD,friendSchema)
    frienddf.createOrReplaceTempView("friends")
    import sc.sqlContext.implicits._
    //sql
    val results1=sc.sql("SELECT friend1,friend2,size(mutual) as friendscount FROM friends").sort($"friendscount".desc).limit(10)
    val userdetails=userdata.map(line=>line.split(",")).map(x=>(x(0),x(1),x(2),x.slice(3,8).mkString(","))).map(attributes=>Row(attributes._1,attributes._2,attributes._3,attributes._4))
    val userSchema=new StructType().add(StructField("userid",StringType,true)).add(StructField("firstname",StringType,true))
                                   .add(StructField("lastname",StringType,true)).add(StructField("address",StringType,true))
    val userDf=sc.createDataFrame(userdetails,userSchema)
    userDf.createOrReplaceTempView("user")
    results1.createOrReplaceTempView("mutualfriend")
    val results=sc.sql("SELECT f.friendscount,user1.firstname,user1.lastname,user1.address,user2.firstname,user2.lastname,user2.address"+
                       " from mutualfriend f inner join user user1 inner join user user2"+
                       " where f.friend1=user1.userid and f.friend2=user2.userid").collect()
    val resultsRDD=sc.sparkContext.parallelize(results).map(x=>x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)+"\t"+x(4)+"\t"+x(5)+"\t"+x(6))
    resultsRDD.saveAsTextFile(args(2))

  }
}
