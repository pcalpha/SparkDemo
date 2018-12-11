package cn.com.pcalpha.spark_demo.example

import java.io.{File, FileWriter}
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

object AvgAgeCalculator {
  val wordSeparator = " "
  val lineSeparator = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {
    avg()
  }

  def avg(): Unit = {
    val conf = new SparkConf().setAppName("Avg age") setMaster ("local")
    val context = new SparkContext(conf)
    var rdd = context.textFile("testFile\\sample_age_data.txt")

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
    val writer = new FileWriter(new File("testFile\\sample_age_data.txt"), false)
    for (i <- 1 to 100000) {
      writer.write(i + wordSeparator + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
