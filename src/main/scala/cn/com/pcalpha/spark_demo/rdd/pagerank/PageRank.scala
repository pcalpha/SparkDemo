package cn.com.pcalpha.spark_demo.rdd.pagerank

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * TO TEST
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val links = sc.objectFile[(String, Seq[String])](" links")
      .partitionBy(new HashPartitioner(100))
      .persist()

    // 将每个页面的排序值初始化为1.0；由于使用mapValues，生成的RDD的分区方式会和"links"的一样
    var ranks = links.mapValues(v => 1.0) //运行10轮PageRank迭代
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap{
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    } // 写出最终排名
    ranks.saveAsTextFile(" ranks")
  }
}
