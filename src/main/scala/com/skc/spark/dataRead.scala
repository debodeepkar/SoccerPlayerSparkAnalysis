package com.skc.spark

object dataRead {


  val sc = new SparkContext("local[*]", "dataRead")
  val df = spark.read.csv("data/players.csv")
  df.printSchema()

}
