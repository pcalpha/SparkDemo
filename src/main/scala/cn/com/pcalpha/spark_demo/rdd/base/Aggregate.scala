package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate接收一个初始化值和两个函数。
  * seqOp函数用于聚集每一个分区，
  * combOp用于聚集所有分区聚集后的结果。
  */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    var result = sc.parallelize(List(2,5,8,1,2,6,9,4,3,5),3)
    .aggregate((0,0))(
      // seqOp
      (acc, number) => (acc._1+number, acc._2+1),
      // combOp
      (par1, par2) => (par1._1+par2._1, par1._2+par2._2)
    )
    println(result)
  }
}
