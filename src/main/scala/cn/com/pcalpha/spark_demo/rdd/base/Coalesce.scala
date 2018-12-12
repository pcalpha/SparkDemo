package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 重新分区
  * 将多个块组合成n个大的列表
  *
  *
  * 现在假设RDD有X个分区,需要重新划分成Y个分区
  *
  * 1.如果X<Y,说明x个分区里有数据分布不均匀的情况,利用HashPartitioner把x个分区重新划分成了y个分区,
  * 此时,需要把shuffle设置成true才行,因为如果设置成false,不会进行shuffle操作,此时父RDD和子RDD之间是窄依赖,这时并不会增加RDD的分区.
  *
  * 2.如果X>Y,需要先把x分区中的某些个分区合并成一个新的分区,然后最终合并成y个分区,此时,需要把coalesce方法的shuffle设置成false.
  *
  */
object Coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)
    rdd1.glom().collect().foreach(e=>{e.foreach(print);println()})//[1,[2,3],[4,5]]
    println("********************************")
    /**
      * X<Y
      */
    rdd1.coalesce(4).glom().collect().foreach(e=>{e.foreach(print);println()})//[1,[2,3,4,5]]
    println("==============")
    //等价 rdd1.repartition(4).glom().collect().foreach(e=>{e.foreach(print);println()})//
    rdd1.coalesce(4,true).glom().collect().foreach(e=>{e.foreach(print);println()})

    println("********************************")
    /**
      * X>Y
      */
    rdd1.coalesce(2).glom().collect().foreach(e=>{e.foreach(print);println()})//[1,[2,3,4,5]]
    println("==============")
    //等价 rdd1.repartition(4).glom().collect().foreach(e=>{e.foreach(print);println()})//
    rdd1.coalesce(2,true).glom().collect().foreach(e=>{e.foreach(print);println()})

  }
}
