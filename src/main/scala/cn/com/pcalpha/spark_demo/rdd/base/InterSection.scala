package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求交集
 */
object InterSection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(1,2,3));
    var rdd2 = sc.parallelize(List(2,3,4));
    rdd1.intersection(rdd2).collect().foreach(println)
  }
}
