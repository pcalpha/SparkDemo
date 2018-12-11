package cn.com.pcalpha.spark_demo.example

import java.io.{File, FileWriter}
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 该案例中，我们将假设我们需要统计一个 1000 万人口的所有人的平均年龄，
  * 当然如果您想测试 Spark 对于大数据的处理能力，您可以把人口数放的更大，
  * 比如 1 亿人口，当然这个取决于测试所用集群的存储容量。
  * 假设这些年龄信息都存储在一个文件里，并且该文件的格式如下，第一列是 ID，第二列是年龄。
  */
object AvgAgeCalculator {
  val wordSeparator = " "
  val lineSeparator = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {
    //先生成数据再计算
    //generator()
    avg()
  }

  def avg(): Unit = {
    val conf = new SparkConf().setAppName("Avg age") setMaster ("local")
    val context = new SparkContext(conf)
    var rdd = context.textFile("exampleFile\\sample_age_data.txt")

    val count = rdd.count()
    val totalAge = rdd.map(line=>(line.split(wordSeparator)(1)))
      .map(age=>Integer.parseInt(age))
      .collect()
      .reduce((x,y)=>x+y)
    println(totalAge.toDouble/count.toDouble)
  }

  def generator(): Unit = {
    val conf = new SparkConf().setAppName("Word Count") setMaster ("local")
    val context = new SparkContext(conf)

    var rand = new Random();
    val writer = new FileWriter(new File("exampleFile\\sample_age_data.txt"), false)
    for (i <- 1 to 100000) {
      writer.write(i + wordSeparator + rand.nextInt(100))
      writer.write(lineSeparator)
    }
    writer.flush()
    writer.close()
  }
}
