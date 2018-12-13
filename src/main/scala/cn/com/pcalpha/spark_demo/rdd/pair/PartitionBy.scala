package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)

    d1.partitionBy(new CustomPartitioner()).glom().foreach(e=>{e.foreach(print);println()});
  }


  class CustomPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      if(key=="Fred"){
        0
      }else if(key =="Wilma"){
        1
      }else{
        2
      }
    }
  }
}
