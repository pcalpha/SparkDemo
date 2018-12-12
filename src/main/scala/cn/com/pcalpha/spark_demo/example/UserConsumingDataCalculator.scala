package cn.com.pcalpha.spark_demo.example

/**
  *通过用户数据和交易订单数据，关联查询用户和订单信息
  */
object UserConsumingDataCalculator {
  private val wordSeparator = " "
  private val lineSeparator = System.getProperty("line.separator")
  private val PEOPLE_FILE_PATH = "exampleFile\\sample_user_data.txt"
  private val CONSUME_FILE_PATH = "exampleFile\\sample_consume_data.txt"
  private val ROLE_ID_ARRAY = Array[String]("ROLE001", "ROLE002", "ROLE003", "ROLE004", "ROLE005")
  private val REGION_ID_ARRAY = Array[String]("REG001", "REG002", "REG003", "REG004", "REG005")
  private val MAX_USER_AGE = 60

  def main(args: Array[String]): Unit = {
    //生成用户数据
    //userDataGenetrator();
    //生成订单数据
    //consumeDataGenetrator()
    calculate()

  }

  def calculate(): Unit = {
    val conf = new SparkConf().setAppName("Avg age") setMaster ("local")
    val context = new SparkContext(conf)

    var userDataRDD = context.textFile("exampleFile\\sample_user_data.txt")
    var orderDataRDD = context.textFile("exampleFile\\sample_consume_data.txt")

    val sqlCtx = new SQLContext(context)
    import sqlCtx.implicits._

    val userDF = userDataRDD
      .map(_.split(" "))
      .map(u => User(u(0), u(1), u(2).toInt,u(3),u(4),u(5)))
      .toDF()
    userDF.createOrReplaceTempView("user")
    userDF.persist(StorageLevel.MEMORY_ONLY_SER)

    val orderDF = orderDataRDD
      .map(_.split(" "))
      .map(o=>Order(o(0),o(1),o(2).toInt,o(3).toInt,o(4)))
      .toDF()
    orderDF.createOrReplaceTempView("orders")
    orderDF.persist(StorageLevel.MEMORY_ONLY_SER)

//    val count = orderDF
//      .filter(orderDF("orderDate").contains("2015"))
//      .join(userDF, orderDF("userID").equalTo(userDF("userID")))
//      .count()
//    println(count)

//    val countOfOrders2014 = sqlCtx.sql("SELECT * FROM orders where orderDate like '2014%'").count()
//    println(countOfOrders2014)

//    val countOfOrdersForUser1 = sqlCtx.sql("""SELECT o.orderID,o.productID, o.price,u.userID FROM orders o,user u where u.userID =3 and u.userID = o.userID""").show()
//    println(countOfOrdersForUser1)

    val orderStatsForUser10 = sqlCtx.sql("""
                                        SELECT max(o.price) as maxPrice,min(o.price) as minPrice,avg(o.price) as avgPrice,u.userID
                                        FROM orders o,user u
                                        where u.userID = 3
                                        and u.userID = o.userID
                                        group by u.userID""")
    orderStatsForUser10.show()


  }

  def userDataGenetrator(): Unit = {
    var writer: FileWriter = null
    try {
      writer = new FileWriter(PEOPLE_FILE_PATH)
      val rand = new Random()
      for (i <- 1 to 10000) {
        //generate the gender of the user
        var gender = if (rand.nextBoolean()) "M" else "F"
        //
        var age = rand.nextInt(MAX_USER_AGE)
        if (age < 10) {
          age = age + 10
        }
        //generate the registering date for the user
        var year = rand.nextInt(16) + 2000
        var month = rand.nextInt(12) + 1
        //to avoid checking if it is a valid day for specific month
        //we always generate a day which is no more than 28
        var day = rand.nextInt(28) + 1
        var registerDate = year + "-" + month + "-" + day
        //generate the role of the user
        var roleIndex: Int = rand.nextInt(ROLE_ID_ARRAY.length)
        var role = ROLE_ID_ARRAY(roleIndex)
        //generate the region where the user is
        var regionIndex: Int = rand.nextInt(REGION_ID_ARRAY.length)
        var region = REGION_ID_ARRAY(regionIndex)

        writer.write(
          i + " "
            + gender + " "
            + age + " "
            + registerDate + " "
            + role + " "
            + region)
        writer.write(lineSeparator)
      }
      writer.flush()
    } catch {
      case e: Exception => println("Error occurred:" + e)
    } finally {
      if (writer != null)
        writer.close()
    }
    println("User Data File generated successfully.")
  }


  // we suppose only 10 kinds of products in the consuming data
  private val PRODUCT_ID_ARRAY = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  // we suppose the price of most expensive product will not exceed 2000 RMB
  private val MAX_PRICE = 2000
  // we suppose the price of cheapest product will not be lower than 10 RMB
  private val MIN_PRICE = 10

  def consumeDataGenetrator(): Unit = {
    var writer: FileWriter = null
    try {
      writer = new FileWriter(CONSUME_FILE_PATH, false)
      val rand = new Random()
      for (i <- 1 to 10000) {
        //generate the buying date
        var year = rand.nextInt(16) + 2000
        var month = rand.nextInt(12) + 1
        //to avoid checking if it is a valid day for specific
        // month,we always generate a day which is no more than 28
        var day = rand.nextInt(28) + 1
        var recordDate = year + "-" + month + "-" + day
        //generate the product ID
        var index: Int = rand.nextInt(PRODUCT_ID_ARRAY.length)
        var productID = PRODUCT_ID_ARRAY(index)
        //generate the product price
        var price: Int = rand.nextInt(MAX_PRICE)
        if (price == 0) {
          price = MIN_PRICE
        }
        // which user buys this product
        val userID = rand.nextInt(10000) + 1
        writer.write(i + " "
          + recordDate + " "
          + productID + " "
          + price + " "
          + userID)
        writer.write(lineSeparator)
      }
      writer.flush()
    } catch {
      case e: Exception => println("Error occurred:" + e)
    } finally {
      if (writer != null)
        writer.close()
    }
    println("Consuming Data File generated successfully.")
  }


  case class User(userID: String, gender: String, age: Int, registerDate: String, role: String, region: String)

  case class Order(orderID: String, orderDate: String, productID: Int, price: Int, userID: String)

}
