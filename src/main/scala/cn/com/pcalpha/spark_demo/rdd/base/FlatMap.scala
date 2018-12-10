package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(List(1,2,3),List(3,4,5),List(5,6,7)))
    rdd1.flatMap(e=>e).foreach(println)


    var rdd2 = sc.parallelize(List(("fruit", "apple,banana,pear"), ("animal", "pig,cat,dog,tiger")))
    rdd2.flatMapValues(_.split(",")).foreach(println)

    var rdd3 = sc.parallelize(List(("fruit", List(1,2,3)), ("animal", List(3,4,5))))
    rdd3.flatMapValues(e=>e).foreach(println)
  }
}
