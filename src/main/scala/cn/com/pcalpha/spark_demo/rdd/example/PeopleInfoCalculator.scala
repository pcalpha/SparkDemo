package cn.com.pcalpha.spark_demo.rdd.example

import java.io.{File, FileWriter}
import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本案例假设我们需要对某个省的人口 (1 亿) 性别还有身高进行统计，需要计算出男女人数，
  * 男性中的最高和最低身高，以及女性中的最高和最低身高。
  * 本案例中用到的源文件有以下格式, 三列分别是 ID，性别，身高 (cm)。
  */
object PeopleInfoCalculator {
  val wordSeparator = " "
  val lineSeparator = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {
    //先生成数据再计算
    //generator()
    calculate()

  }

  def calculate(): Unit = {
    val conf = new SparkConf().setAppName("PeopleInfo") setMaster ("local[3]")
    val context = new SparkContext(conf)

    val rdd = context.textFile("exampleFile\\sample_people_data.txt");
    val femaleData = rdd
      .filter(line=>line.split(" ")(1)=="F")
      .map(line=>(line.split(" ")(1),line.split(" ")(2)))

    val femaleCount = femaleData.count();
    val femmaleAge = femaleData.map(data=>data._2)
    val femmaleMaxAge = femmaleAge.max();
    val femmaleMinAge = femmaleAge.min();
    println("female maxage "+femmaleMaxAge)
    println("female minage "+femmaleMinAge)


    val maleData = rdd
      .filter(line=>line.split(" ")(1)=="M")
      .map(line=>(line.split(" ")(1),line.split(" ")(2)))
    val maleCount = maleData.count()
    val maleAge = maleData.map(data=>data._2)
    val maleMaxAge =maleAge.max()
    val maleMinAge = maleAge.min()
    println("male maxage "+maleMaxAge)
    println("male minage "+maleMinAge)


  }

  def generator(): Unit = {
    val conf = new SparkConf().setAppName("Avg age") setMaster ("local")
    val context = new SparkContext(conf)

    val writer = new FileWriter(new File("exampleFile\\sample_people_data.txt"), false)
    var rand = new Random();
    for (i <- 1 to 10000) {
      var gender = if (rand.nextBoolean()) "M" else "F"
      var height = 0;
      if(gender=="M"){
        height = 150 + rand.nextInt(50)
      }else if(gender=="F"){
        height = 140 + rand.nextInt(30)
      }


      writer.write(i + wordSeparator + gender + wordSeparator + height)
      writer.write(lineSeparator)
    }
    writer.flush()
    writer.close()
  }
}
