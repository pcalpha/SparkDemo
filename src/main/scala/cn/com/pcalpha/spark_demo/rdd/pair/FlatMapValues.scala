package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd3 = sc.parallelize(List(("fruit", List(1,2,3)), ("animal", List(3,4,5))))
    rdd3.flatMapValues(e=>e).foreach(println)
  }
}
