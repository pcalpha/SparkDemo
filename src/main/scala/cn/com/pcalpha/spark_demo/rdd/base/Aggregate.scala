package cn.com.pcalpha.spark_demo.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate接收一个初始化值和两个函数
  * seqOp函数用于聚集一个分区内的数据
  * combOp用于聚集所有分区聚集后的结果
  */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)


    /**
      * 假设数据分为三个分区
      * (2,5,8) (1,2,6) (9,4,3,5)
      * 初始值 (sum,count)
      * (0,0) (0,0) (0,0)
      *
      * SeqOp操作 同一个分区内数据处理
      * (0+2,0+1)           (0+1,0+1)             (0+9,0+1)
      * (0+2+5,0+1+1)       (0+1+2,0+1+1)         (0+9+4,0+1+1)
      * (0+2+5+8,0+1+1+1)   (0+1+2+6,0+1+1+1)     (0+9+4+3,0+1+1+1)
      *                                           (0+9+4+3+5,0+1+1+1+1)
      * SeqOp结果
      * (15,3) (9,3) (21,4)
      *
      * CombOp操作  汇总所有分区数据
      * (15+9+21,3+3+4)
      * CombOp结果
      * (45,10)
      */
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
