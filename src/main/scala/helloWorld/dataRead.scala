package helloWorld
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object dataRead extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder.master("local[*]").appName("dataRead").getOrCreate()
  //val sc = new spark.sparkContext("local[*]", "dataRead")

  /*val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)*/


  val df = spark.read.option("header",true).csv("/Users/debodeepkar/IdeaProjects/SoccerPlayerSparkAnalysis/data/players.csv")

  df.show(5)


  df.createOrReplaceTempView("soccer")

  spark.sql("select long_name from soccer").show()






}
