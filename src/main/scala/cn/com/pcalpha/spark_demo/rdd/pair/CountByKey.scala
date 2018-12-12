package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd2 = sc.parallelize(List(("a",1),("b",1),("c",1),("a",3)))
    rdd2.countByKey().foreach(println)

  }
}
