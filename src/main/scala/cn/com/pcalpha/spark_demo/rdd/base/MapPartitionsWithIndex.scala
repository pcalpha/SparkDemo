package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd1:RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7),2);
    rdd1.mapPartitionsWithIndex(function).glom().foreach(e=>{e.foreach(println);println()})
  }

  def function(partionIndex:Int,iterator: Iterator[Int]):Iterator[Int] ={
    var res = for(e<-iterator) yield e;
    if(partionIndex==0){
      res = for(e<-iterator) yield e*2
    }
    return res
  }
}
