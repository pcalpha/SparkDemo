package cn.com.pcalpha.spark_demo.hbase

import java.util.Base64

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.protobuf.ProtobufUtil


object Query {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local"))

    val conf = HBaseConfiguration.create()
    //设置查询的表名
    conf.set("hbase.zookeeper.quorum","master:2181")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, "User")


    val startRowkey="1"
    val endRowkey="4"
    //开始rowkey和结束一样代表精确查询某条数据

    //组装scan语句
    val scan=new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
    scan.setCacheBlocks(false)
    /*    scan.addFamily(Bytes.toBytes("ks"));
        scan.addColumn(Bytes.toBytes("ks"), Bytes.toBytes("data"))*/

    //将scan类转化成string类型
    val proto= ProtobufUtil.toScan(scan)
    val ScanToString = Base64.getEncoder.encodeToString(proto.toByteArray());
    conf.set(TableInputFormat.SCAN,ScanToString)

    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()


    //遍历输出
    stuRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    })
  }

}
