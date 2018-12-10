package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9))
    rdd1.filter(e=>e%2==0).foreach(println)

    println(rdd1.first())
  }

}
