package cn.com.pcalpha.spark_demo.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object UserBehaviorMsgConsumer {
  private val zkServers = "10.33.49.160:2181"
  private val processingInterval = 2
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator").setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
    ssc.checkpoint(checkpointDir)//using updateStateByKey asks for enabling checkpoint

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.33.49.160:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      "group.id" -> msgConsumerGroup,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("user-behavior-topic")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //val kafkaStream = KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map("user-behavior-topic" -> 3))
    val msgDataRDD = kafkaStream.map(_.value())
    //for debug use only
    //println("Coming data in this interval...")
    //msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
    val popularityData = msgDataRDD.map {
      msgLine => {
        val dataArr: Array[String] = msgLine.split("\\|")
        val pageID = dataArr(0)
        val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
        (pageID, popValue)
      }
    }
    //sum the previous popularity value and current value
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }

    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      true,
      initialRDD
    )
    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    stateDstream.checkpoint(Duration(8 * processingInterval.toInt * 1000))
    //after calculation, we need to sort the result and only show the top 10 hot pages
    stateDstream.foreachRDD{
      rdd => {
        val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
        val topKData = sortedData.take(10).map { case (v, k) => (k, v) }
        topKData.foreach(x => {
          println(x)
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
