package cn.com.pcalpha.spark_demo.sql.dataset.example

import java.io.{File, FileWriter}
import java.util.Random
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 目标：
  * 用 SQL 语句的方式统计男性中身高超过 180cm 的人数。
  * 用 SQL 语句的方式统计女性中身高超过 160cm 的人数。
  * 对人群按照性别分组并统计男女人数。
  * 用类 RDD 转换的方式对 DataFrame 操作来统计并打印身高大于 210cm 的前 50 名男性。
  * 对所有人按身高进行排序并打印前 50 名的信息。
  * 统计男性的平均身高。
  * 统计女性身高的最大值。
  */
object PeopleInfoCalculator3 {
  private val schemaString = "id,gender,height"
  private val wordSeparator = " "
  private val lineSeparator = System.getProperty("line.separator")

  def main(args: Array[String]): Unit = {
    //先生成数据再计算
    //generator()
    calculate()
  }

  def calculate(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val peopleDS = spark
      .sparkContext
      .textFile("exampleFile\\sample_people_data2.txt")
      .map(_.split(" "))
      .map(eachRow => Person(eachRow(0).trim.toInt, eachRow(1), eachRow(2).trim.toInt))
      .toDF().as[Person]


    /**
      * spark sql
      */
    val sqlCtx = peopleDS.sqlContext
    peopleDS.createOrReplaceTempView("people")
    //用 SQL 语句的方式统计男性中身高超过 180cm 的人数。
    val higherMale180 = sqlCtx.sql("select id,gender, height from people where height > 180 and gender='M'")
    println(higherMale180.count())

    //用 SQL 语句的方式统计女性中身高超过 160cm 的人数。
    val higherFemale160 = sqlCtx.sql("select id,gender, height from people where height > 160 and gender='F'")
    println(higherFemale160.count())

    /**
      * dataframe
      */


    // 对人群按照性别分组并统计男女人数。
    peopleDS.groupBy(peopleDS("gender")).count().show()
    println("People Count Grouped By Gender")

    //用类 RDD 转换的方式对 DataFrame 操作来统计并打印身高大于 180cm 的前 50 名男性。
    peopleDS.filter(peopleDS("gender").equalTo("M")).filter(peopleDS("height") > 180).show(50)

    // 对所有人按身高进行排序并打印前 50 名的信息。
    peopleDS.sort(peopleDS("height").desc).take(50).foreach { row => println(row) }

    //统计男性的平均身高
    peopleDS.filter(peopleDS("gender").equalTo("M")).agg(Map("height" -> "avg")).show()

    //统计女性身高的最大值
    peopleDS.filter(peopleDS("gender").equalTo("F")).agg("height" -> "max").show()

  }


  def generator(): Unit = {
    val conf = new SparkConf().setAppName("Avg age") setMaster ("local")
    val context = new SparkContext(conf)

    val writer = new FileWriter(new File("exampleFile\\sample_people_data2.txt"), false)
    var rand = new Random()
    for (i <- 1 to 10000) {
      var gender = if (rand.nextBoolean()) "M" else "F"
      var height = 0;
      if (gender == "M") {
        height = 150 + rand.nextInt(50)
      } else if (gender == "F") {
        height = 140 + rand.nextInt(30)
      }

      writer.write(i + wordSeparator + gender + wordSeparator + height)
      writer.write(lineSeparator)
    }
    writer.flush()
    writer.close()
  }

  case class Person(id:Int,gender:String,height:Int){

  }
}
