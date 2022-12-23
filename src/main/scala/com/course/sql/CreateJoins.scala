package com.course.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, lit, row_number}
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

    val empWithDeptDF = employees.alias("emp").join(
      broadcast(departments.alias("dep")),
      employees("dept_id") === departments("dept_id"),
      "left"
    )

    val empWithDeptDF2 = employees.alias("emp").join(
      broadcast(departments.alias("dep")),
      employees("dept_id") === departments("dept_id"),
      "left"
    )

    ////UNIONS
    val dfUnion = empWithDeptDF.union(empWithDeptDF2)// equivalente a un union en SQL
    //val dfUnionAll = empWithDeptDF.union(empWithDeptDF2).show() // esto es equivalente a un union all en sql
    //val dfUnionAll = empWithDeptDF.unionAll(empWithDeptDF2).show() // esto es equivalente a un union all en sql, pero esta obsoleto

    // Eliminar duplicados sin utilizar el distinct()
//    val windowSpecAgg = Window.partitionBy("emp.emp_id","emp.dept_id").orderBy("emp.emp_id")
//    val dfUnion = empWithDeptDF.union(empWithDeptDF2)
//    val dfUnionWithRow = dfUnion.withColumn("row", row_number.over(windowSpecAgg))
//      .filter(col("row")===1)
//      .select(col("*"), lit("Hola").as("newCol"))

    print(">>>> Explain")
    dfUnion.explain()
    dfUnion.show()
  }
}
