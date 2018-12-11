package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)

    d1.aggregateByKey((0.0,0.0))(
      (element,number)=>(element._1+number,element._2+1),
      (part1,part2)=>(part1._1+part2._1,part1._2+part2._2)
    ).collect().foreach(println)
  }
}
