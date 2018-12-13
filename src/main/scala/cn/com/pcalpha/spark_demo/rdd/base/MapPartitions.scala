package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * map是对rdd中的每一个元素进行操作，
  * mapPartitions(foreachPartition)则是对rdd中的每个分区的迭代器进行操作
  * 如果在map过程中需要频繁创建额外的对象(例如将rdd中的数据通过jdbc写入数据库,
  * map需要为每个元素创建一个链接，而mapPartition为每个partition创建一个链接),
  * 则mapPartitions效率比map高的多。
  *
  */
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
     for(e<-iterator) yield e*2
  }
}
