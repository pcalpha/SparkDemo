package cn.com.pcalpha.spark_demo.rdd.pair.join

import org.apache.spark.{SparkConf, SparkContext}

object SubtractByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(("Fred", 88.0),("Wilma", 93.0)))
    val rdd2 = sc.parallelize(Array(("Fred", 91.0)))

    rdd1.subtractByKey(rdd2).collect().foreach(println)


  }
}
