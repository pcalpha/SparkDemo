package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 生成相应的key，相同的放在一起
  */
object GroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 =sc.parallelize( Array(1,2,3,4,5,6))
    rdd1.groupBy(e=>{if (e%2==0)"even" else "odd"}).foreach(println)

  }
}
