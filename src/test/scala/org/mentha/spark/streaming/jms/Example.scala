package org.mentha.spark.streaming.jms

import java.util.Date

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object Example {

  case class Record(
    messageId: String,
    timestamp: Long,
    content: String
  )

  object ActiveMQStream {

    val brokerURL = "tcp://localhost:61616"
    val username = "admin"
    val password = "admin"
    val queueName = "testQueue?consumer.prefetchSize=128"

    val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerURL)
    cf.setOptimizeAcknowledge(true)

    def receiver() = new JmsReceiver[Record](
      queueName = queueName,
      transformer = msg => Record(
        messageId = msg.getJMSMessageID(),
        timestamp = msg.getJMSTimestamp(),
        content = msg.asInstanceOf[javax.jms.TextMessage].getText()
      ),
      connectionProvider = () => {
        cf.createConnection(username, password)
      }
    )

    def stream(ssc: StreamingContext) = ssc.receiverStream(receiver())

  }

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[8]")

    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    val stream: DStream[Record] = ActiveMQStream.stream(ssc)
      .union(ActiveMQStream.stream(ssc)) // It's possible to union several streams

    val basePath = "records/parquet"
    stream
      .repartition(1)
      .foreachRDD { (rdd, time) => {
        if (!rdd.isEmpty()) {
          val count: Long = rdd.count()
          val postfix: String = new java.text.SimpleDateFormat("yyyy-MM-dd/hh-mm-ss").format(new Date(time.milliseconds))
          SQLContext
            .getOrCreate(rdd.sparkContext)
            .createDataFrame(rdd)
            .write
            .mode(SaveMode.ErrorIfExists)
            .parquet(basePath + "/" + postfix)
        }
      }}

  }

}
