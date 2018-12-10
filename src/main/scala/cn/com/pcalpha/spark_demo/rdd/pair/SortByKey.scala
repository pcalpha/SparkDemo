package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd3 = sc.parallelize(List(("fruit", 1), ("animal", 2)))
    rdd3.sortByKey().collect().foreach(println)
  }
}
