package cn.com.pcalpha.spark_demo.sql.dataset

import org.apache.spark
import org.apache.spark.sql.SparkSession


object TestDataSet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val caseClassDS = Seq(Person("id1","Andy", 32)).toDS()
    caseClassDS.show()

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "exampleFile/people.json"
    val peopleDS = spark.read.json(path).as[Person]

    peopleDS.select("name").where("age>20").show()
  }

  case class Person(id:String, name:String, age:Long){


  }
}
