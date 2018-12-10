package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

object CollectAsMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List((1,2),(3,4)))
    rdd1.collectAsMap().foreach(println);

    var rdd2 = sc.parallelize(List((1,(3,4)),(5,6)))
    rdd2.collectAsMap().foreach(println);

  }
}
