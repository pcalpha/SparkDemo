package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

object Count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd = sc.parallelize(List(1,2,3,4,5))
    println(rdd.count())

    println("===========")

    var rdd3 = sc.parallelize(List("a","b","c","a","b"))
    rdd3.countByValue().foreach(println)
  }

}
