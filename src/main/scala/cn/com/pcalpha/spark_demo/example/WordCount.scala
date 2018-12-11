package cn.com.pcalpha.spark_demo.example

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex

/**
  * 计算文本文件中出现的单词的数量
  */
object WordCount {
  val PARTTERN: Regex ="^\\w+$".r;

  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName("WordCount").setMaster("local[3]")
    val context = new SparkContext(conf)
    val textFile = context.textFile("exampleFile\\Apache Kafka.html");
    textFile.flatMap(line=>line.split(" "))
      .filter(word=>isValidateWord(word))
      .map(word=>(word,1))
      .reduceByKey((x,y)=>x+y)
      .map{case(x,y)=>(y,x)}
      .sortByKey(false)
      .top(10)
      .foreach(e=>println(e._1+" "+e._2))

    print("==============")
    var max = textFile.flatMap(line=>line.split(" "))
      .filter(word=>isValidateWord(word))
      .map(word=>(word,1))
      .reduceByKey((x,y)=>x+y)
      .map{case(x,y)=>(y,x)}
      .sortByKey(false).first()
    print(max)
  }

  def isValidateWord(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)

    if (options.isEmpty) {
      false
    } else {
      true
    }
  }
}
