package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合
  * RDD内部的元素也会按照key进行聚合
  */
object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var x = sc.parallelize(Array((1, "tom"),(2, "lily")))//(id,name)
    var y = sc.parallelize(Array((1, 98),(2, 95),(1, 91),(2,89)))//(id,score)

    x.cogroup(y).collect().foreach(println)
  }

}
