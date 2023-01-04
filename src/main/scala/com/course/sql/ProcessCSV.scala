package com.course.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit}

object ProcessCSV {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    val dfData = spark.read.csv("src/main/resources/data/input/sample.csv")
//    spark.table("bd.table")
//    dfData.show()

    val dfFiltered = dfData.select(
      col("_c1").as("age"),
      col("_c2").as("Gender"),
      col("_c3").as("Country"),
      col("_c4").as("state"),
    ).filter(
      col("age") < 40
    )

    val dfGrouped = dfFiltered
      .groupBy("Country")
      .agg(count("Country").as("count_by_country"))
      .withColumn("newColumn",lit(1))

    dfGrouped.show()

//    dfGrouped
//      .repartition(1)
//    .write
////      .mode("append")
//      .mode("overwrite")
//      .parquet("src/main/resources/data/output/countries2.parquet")

//    dfGrouped.write.insertInto("database.tabla")
//    dfGrouped.write.format("avro").save("src/main/resources/data/output/countriesAvro.avro")

  }
}
