package com.course.sql

import org.apache.spark.sql.SparkSession

object CreateDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    import spark.implicits._
    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd = spark.sparkContext.parallelize(data)

    val dfFromRDD1 = rdd.toDF(columns:_*)
//    val dfFromRDD1 = rdd.toDF(columns:_*)

    dfFromRDD1.printSchema()

    dfFromRDD1.show(10)
  }
}
