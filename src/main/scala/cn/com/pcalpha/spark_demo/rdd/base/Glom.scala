package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将一个一维横向列表，划分为多个块
  */
object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var data = List(1,2,3,4,5,6);
    var rdd = sc.parallelize(data,1)
    rdd.glom().collect().foreach(e=>{e.foreach(print);println()});//[[1,2,3,4,5,6]]

    var rdd2 = sc.parallelize(data,2)
    rdd2.glom().collect().foreach(e=>{e.foreach(print);println()});//[[1,2,3],[4,5,6]]


  }

}
