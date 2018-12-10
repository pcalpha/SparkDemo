package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔积
  */
object Cartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //Cartesian
    var rdd1 = sc.parallelize(List(1,2))
    var rdd2 = sc.parallelize(List(3,4,5))
    rdd1.cartesian(rdd2).collect().foreach(println);
  }

}
