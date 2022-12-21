package com.course.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CreateJoins {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()


    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    employees.printSchema()
    employees.show()

    val departments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    departments.printSchema()
    departments.show()

    val empWithDeptDF = employees.join(departments, employees("emp_dept_id") === departments("dept_id"), "inner")

    empWithDeptDF.show()
  }
}
