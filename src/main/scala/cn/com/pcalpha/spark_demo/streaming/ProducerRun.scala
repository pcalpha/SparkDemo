package cn.com.pcalpha.spark_demo.streaming

object ProducerRun {
  def main(args: Array[String]): Unit = {
    var thread = new Thread(new UserBehaviorMsgProducer()).start();
  }
}
