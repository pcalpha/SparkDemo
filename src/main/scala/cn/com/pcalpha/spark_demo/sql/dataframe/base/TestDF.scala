package cn.com.pcalpha.spark_demo.sql.dataframe.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TestDF {
  private val schemaString = "id,name,age"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val schemaArray="id,name,age".split(",")
    val schema = StructType(schemaArray.map(fieldName => StructField(fieldName, StringType, true)))
    /**
      * json可以不设置scheme
      */
    //val df = spark.read.json("exampleFile\\people.json")
    val df = spark.read.schema(schema).csv("exampleFile\\people.csv")

    //
    //df.select("name","age").show()
    //df.agg("age"->"max").show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT name,age FROM people").show()
  }
}
