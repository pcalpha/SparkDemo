package cn.com.pcalpha.spark_demo.rdd.pair

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    //定义一个元组类型(科目计数器,分数)
    //计算平均数
    type MVType = (Int, Double)
    d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }.foreach(println)


    //以下两个例子 combineByKey和aggregateByKey的结果相同
//    d1.combineByKey(
//      score => (1, score),
//      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
//      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
//    ).foreach(println)

//    d1.aggregateByKey((0.0,0.0))(
//      (element,number)=>(element._1+number,element._2+1),
//      (part1,part2)=>(part1._1+part2._1,part1._2+part2._2)
//    ).collect().foreach(println)
  }
}
