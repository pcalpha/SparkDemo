package cn.com.pcalpha.spark_demo.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object Insert {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local"))

    val conf = HBaseConfiguration.create()
    //设置查询的表名
    conf.set("hbase.zookeeper.quorum","master:2181")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, "User")


    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "User")

    val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))


    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)
  }
}
