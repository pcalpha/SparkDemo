package cn.com.pcalpha.spark_demo.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object TestKafka {
  def main(args: Array[String]): Unit = {
    consumer()
  }


  def consumer(): Unit ={
    val topics = util.Arrays.asList("test")
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.33.49.160:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor")
    props.put("group.id", "test")
    //props.put("auto.offset.reset","earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(topics)
    while(true){
      val records = consumer.poll(100)
      var itor = records.iterator()
      while(itor.hasNext){
        var record = itor.next();
        println(record.key(),record.value())
      }
    }
    consumer.close()
  }


  def producer(): Unit ={
    val brokers_list = "10.33.49.160:9092"
    val topic = "test"
    val properties = new Properties()
    properties.put("group.id", "test")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
    val producer = new KafkaProducer[String, String](properties)
    var num = 0
    for(i<- 1 to 10){
      var msg = "hello"+i
      producer.send(new ProducerRecord(topic,"test",msg))
    }
    producer.flush()
    producer.close()
  }
}
