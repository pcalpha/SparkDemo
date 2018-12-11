package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 重新分区
  * 将多个块组合成n个大的列表
  */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(1,2,3,4,5),3)
    rdd1.glom().collect().foreach(e=>{e.foreach(print);println()})//[1,[2,3],[4,5]]
    rdd1.coalesce(2).glom().collect().foreach(e=>{e.foreach(print);println()})//[1,[2,3,4,5]]
    rdd1.coalesce(2).collect().foreach(print)//

  }
}
