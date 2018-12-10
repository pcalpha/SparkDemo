package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var x = sc.parallelize(Array(("a", 1), ("b", 4)))
    var y = sc.parallelize(Array(("a", 2)))

    x.cogroup(y).collect().foreach(println)
  }

}
