package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1:RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),3)
    rdd1.mapPartitions(function).glom().foreach(println)

    println("=====")
    rdd1.mapPartitions(function).glom().foreach(e=>{e.foreach(println);println()})
  }

  def function(iterator: Iterator[Int]):Iterator[Int] ={
    var res = for(e<-iterator) yield e*2
    return res
  }
}
