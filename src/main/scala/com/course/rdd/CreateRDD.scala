package com.course.rdd

import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Spark course")
      .getOrCreate()

    // Prepare Data

    val data = Array("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")

    // Create RDD
    val rdd = spark.sparkContext.parallelize(data)
    println(rdd.collect())
    println(rdd.collect().mkString("<<>>"))
    Thread.sleep(10000000)
  }

}
