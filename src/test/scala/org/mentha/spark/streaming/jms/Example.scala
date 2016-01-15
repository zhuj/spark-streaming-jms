/*
 * Copyright (C) 2015-2016 the original author or authors.
 * See the LICENCE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mentha.spark.streaming.jms

import java.util.Date
import javax.{jms => jms}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

object Example {

  case class Record(
    messageId: String,
    timestamp: Long,
    content: String
  )

  object ActiveMQStream {

    private val brokerURL = "tcp://localhost:61616"
    private val username = "admin"
    private val password = "admin"

    @inline
    private def buildConnection(): jms.Connection = {
      val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerURL)
      cf.setOptimizeAcknowledge(true)
      cf.createConnection(username, password)
    }

    @inline
    private def transform(message: jms.Message): Record = {
      Record(
        messageId = message.getJMSMessageID(),
        timestamp = message.getJMSTimestamp(),
        content = message.asInstanceOf[javax.jms.TextMessage].getText()
      )
    }

    def queue(): Receiver[Record] = new AbstractJmsReceiver[Record]() {
      val queueName = "testQueue?consumer.prefetchSize=128"
      override protected def buildConnection(): jms.Connection = ActiveMQStream.buildConnection()
      override protected def transform(message: jms.Message): Record = ActiveMQStream.transform(message)
      override protected def buildDestination(session: jms.Session): jms.Destination = session.createQueue(queueName)
    }

    def topic(): Receiver[Record] = new AbstractJmsReceiver[Record]() {
      val topicName = "testQueue?consumer.prefetchSize=128"
      override protected def buildConnection(): jms.Connection = ActiveMQStream.buildConnection()
      override protected def transform(message: jms.Message): Record = ActiveMQStream.transform(message)
      override protected def buildDestination(session: jms.Session): jms.Destination = session.createTopic(topicName)
    }

  }

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[8]")

    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    val stream: DStream[Record] =
    ssc.union(Seq(
      ssc.receiverStream(ActiveMQStream.queue()),
      ssc.receiverStream(ActiveMQStream.topic())
    ))

    val basePath = "records/parquet"
    stream
      .foreachRDD { (rdd, time) => {
        if (!rdd.isEmpty()) {
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
