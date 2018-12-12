package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

/**
  * collectAsMap函数返回所有元素集合，不过该集合是去掉的重复的key的集合，如果元素重该复集合中保留的元素是位置最后的一组
  */
object CollectAsMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List((1,2),(1,4),(2,4)))
    rdd1.collectAsMap().foreach(println)

    println("============")

    var rdd2 = sc.parallelize(List((1,(3,4)),(1,6)))
    rdd2.collectAsMap().foreach(println)

  }
}
