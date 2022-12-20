package com.course.sql

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, dense_rank, max, min, rank, row_number, sum}

object CreateWindowFunctions {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    //Read your CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    df.printSchema()
    df.show()

    /////////////////////
    //ranking functions
    //row_number
    val windowSpec = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number", row_number.over(windowSpec))
      .show()

    //rank
    df.withColumn("rank", rank().over(windowSpec))
      .show()

    //dense_rank
    df.withColumn("dense_rank", dense_rank().over(windowSpec))
      .show()


    /////////////////////
    //aggregate functions
//    val windowSpecAgg = Window.partitionBy("department")
//
//    val aggDF = df.withColumn("row", row_number.over(windowSpec))
//      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
//      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
//      .withColumn("min", min(col("salary")).over(windowSpecAgg))
//      .withColumn("max", max(col("salary")).over(windowSpecAgg))
//      .withColumn("count", count(col("salary")).over(windowSpecAgg))
////      .where(col("row") === 1)
//      .select("row","department", "avg", "sum", "min", "max", "count")
//      .show()
  }
}
