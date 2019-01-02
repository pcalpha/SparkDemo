package cn.com.pcalpha.spark_demo.rdd.example

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * 计算tomcat日志中 INFO，DEBUG级别日志的数量
  */
object LogDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count") setMaster ("local")
    val context = new SparkContext(conf);
    val input = context.textFile("exampleFile\\balarm.balarm-web.debug.log");
    //不同日志级别计数
    input.filter(line => isValidateLogLine(line))
      .map(line => line.split(" ")(1))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(e => println(e))
  }

  //2018-11-17T09:44:27.273+08:00 DEBUG balarm-web [XNIO-2 task-175] [com.hikvision.ga.common.boot.cas.config.HikCasAuthenticationFilter] - serviceTicketRequest = false
  val PARTTERN: Regex ="""^(\S+) (\S+) (\S+) \[.+\] \[.+\] - (\S+) = (\S+)""".r;
  def isValidateLogLine(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)

    if (options.isEmpty) {
      false
    } else {
      true
    }
  }
}
