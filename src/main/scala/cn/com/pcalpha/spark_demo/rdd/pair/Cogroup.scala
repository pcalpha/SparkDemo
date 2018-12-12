package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合
  */
object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var x = sc.parallelize(Array(("a", 1), ("b", 4)))
    var y = sc.parallelize(Array(("a", 2)))

    x.cogroup(y).collect().foreach(println)
  }

}
