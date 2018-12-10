package cn.com.pcalpha.spark_demo.streaming

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class UserBehaviorMsgProducer extends Runnable{
  private val brokerList = "10.33.49.160:9092"
  private val topic = "user-behavior-topic"
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[String, String](this.props)
  private val PAGE_NUM = 100
  private val MAX_MSG_NUM = 3
  private val MAX_CLICK_TIME = 5
  private val MAX_STAY_TIME = 10
  //Like,1;Dislike -1;No Feeling 0
  private val LIKE_OR_NOT = Array[Int](1, 0, -1)
  private val rand = new Random()


  def run(): Unit = {
    while (true) {
      //how many user behavior messages will be produced
      val msgNum = rand.nextInt(MAX_MSG_NUM) + 1
      try {
        //generate the message with format like page1|2|7.123|1
        for (i <- 0 to msgNum) {
          var msg = new StringBuilder()
          msg.append("page" + (rand.nextInt(PAGE_NUM) + 1))
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
          msg.append("|")
          msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat())
          msg.append("|")
          msg.append(LIKE_OR_NOT(rand.nextInt(3)))
          println(msg.toString())
          //send the generated message to broker
          sendMessage(msg.toString())
        }
        println("%d user behavior messages produced.".format(msgNum+1))
      } catch {
        case e: Exception => println(e)
      }
      try {
        //sleep for 5 seconds after send a micro batch of message
        Thread.sleep(5000)
      } catch {
        case e: Exception => println(e)
      }
    }
  }

  def sendMessage(message: String) = {
    try {
      //val data = new KeyedMessage[String, String](this.topic, message);
      val data = new ProducerRecord[String, String](this.topic,message)
      producer.send(data);
    } catch {
      case e:Exception => println(e)
    }
  }





}
