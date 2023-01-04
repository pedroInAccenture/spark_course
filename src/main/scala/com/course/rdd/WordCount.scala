package com.course.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark course")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("src/main/resources/data/input/test.txt")
    println("initial partition count:" + rdd.getNumPartitions)

    val reparRdd = rdd.repartition(4)
    println("re-partition count:" + reparRdd.getNumPartitions)

    reparRdd.collect().foreach(println)

//    // rdd flatMap transformation
    val rdd2 = rdd.flatMap(_.split(" "))
    rdd2.foreach(f => println(f))
//
//    //Create a Tuple by adding 1 to each word
    val rdd3: RDD[(String, Int)] = rdd2.map(palabra => (palabra, 1))
    rdd3.foreach(println)
//
//    //Filter transformation
//    val rdd4 = rdd3.filter(a => a._1.startsWith("a"))
//    rdd4.foreach(println)
//
    //ReduceBy transformation
    val rdd5 = rdd3.reduceByKey(_ + _)
    rdd5.foreach(println)
//
    //Swap word,count and sortByKey transformation
    val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey()
    println("Final Result")
//
//    //Action - foreach
    rdd6.foreach(println)

    //Action - count
    println("Count : " + rdd6.count())
//
//    //Action - first
    val firstRec = rdd6.first()
    println("First Record : " + firstRec._1 + "," + firstRec._2)
//
//    //Action - max
    val datMax = rdd6.max()
    println("Max Record : " + datMax._1 + "," + datMax._2)
//
    //Action - reduce
    val totalWordCount = rdd6.reduce((tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple2._2))
    println("dataReduce Record : " + totalWordCount._1)
    //Action - take
    val data3 = rdd6.take(3)
    data3.foreach(f => {
      println("data3 Key:" + f._1 + ", Value:" + f._2)
    })
//
//    //Action - collect
    val data = rdd6.collect()
    data.foreach(f => {
      println("Key:" + f._1 + ", Value:" + f._2)
    })

    //Action - saveAsTextFile
    rdd5.repartition(4).saveAsTextFile("src/main/resources/data/output/wc")
//
//    Thread.sleep(10000000)
  }

}
