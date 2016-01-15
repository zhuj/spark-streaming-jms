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

import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

class JmsReceiverSuite extends FunSuite with Logging with Eventually with BeforeAndAfterAll {

  private val brokerHost: String = "localhost"
  private val brokerPort: Int = {
    val serverSocket = new java.net.ServerSocket(0)
    try {
      serverSocket.getLocalPort()
    } finally {
      serverSocket.close()
    }
  }

  private val brokerUri: java.net.URI = {
    new java.net.URI(s"tcp://$brokerHost:$brokerPort")
  }

  private lazy val persistenceDir: java.io.File = {
    java.nio.file.Files.createTempDirectory(
      "mentha-spark-jms-test-"
    ).toFile
  }

  private lazy val broker: BrokerService = {
    val broker = new BrokerService()
    broker.setDataDirectoryFile(persistenceDir)
    val connector = new TransportConnector()
    connector.setName("openwire")
    connector.setUri(brokerUri)
    broker.addConnector(connector)
    broker
  }

  override protected def beforeAll(): Unit = {
    require(persistenceDir.exists())
    broker.start()
  }

  override protected def afterAll(): Unit = {
    broker.stop()
    org.apache.commons.io.FileUtils.deleteDirectory(persistenceDir)
  }

  private val batchDuration = Milliseconds(100)
  private val master = "local[4]"
  private val framework = this.getClass.getSimpleName

  private def publish(
    destination: javax.jms.Session => javax.jms.Destination,
    msg: String
  ): Unit = {

    val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerUri)
    cf.setOptimizeAcknowledge(true)
    cf.setSendAcksAsync(true)

    val c = cf.createConnection()

    val session = c.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(destination(session))
    c.start()

    val message = session.createTextMessage(msg)
    producer.send(message)
    c.stop()
    c.close()
  }

  test("jms ActiveMQ Receiver") {

    val sendQueueMessage = "ActiveMQ Test Queue Message"
    val queueName = "testQueue"

    val sendTopicMessage = "ActiveMQ Test Topic Message"
    val topicName = "testTopic"

    val sc = new SparkContext(master, framework)
    val ssc = new StreamingContext(sc, batchDuration)

    val stream: DStream[String] = ssc.union(Seq(
      ssc.receiverStream(JmsReceiverUtils.queue(queueName, brokerUri.toString())),
      ssc.receiverStream(JmsReceiverUtils.topic(topicName, brokerUri.toString()))
    ))

    import JmsReceiverUtils._
    val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
    stream.foreachRDD { rdd => rdd.foreach(x => acc += x) }

    ssc.start()

    // Retry it because we don't know when the receiver will start.
    eventually(timeout(10000 milliseconds), interval(500 milliseconds)) {
      publish(
        session => session.createQueue(queueName),
        sendQueueMessage
      )
      publish(
        session => session.createTopic(topicName),
        sendTopicMessage
      )

      acc.value should contain(sendQueueMessage)
      acc.value should contain(sendTopicMessage)
    }

    ssc.stop()
  }

}

private[jms] object JmsReceiverUtils {

  implicit def setAccum[A]: AccumulableParam[mutable.Set[A], A] = new AccumulableParam[mutable.Set[A], A] {
    def addInPlace(t1: mutable.Set[A], t2: mutable.Set[A]): mutable.Set[A] = {
      t1 ++= t2
      t1
    }
    def addAccumulator(t1: mutable.Set[A], t2: A): mutable.Set[A] = {
      t1 += t2
      t1
    }
    def zero(t: mutable.Set[A]): mutable.Set[A] = {
      new mutable.HashSet[A]()
    }
  }

  def queue(queueName: String, brokerUri: String): Receiver[String] = new JmsQueueReceiver(
    queueName = queueName,
    transformer = msg => msg.asInstanceOf[javax.jms.TextMessage].getText(),
    connectionProvider = () => {
      val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerUri)
      cf.setOptimizeAcknowledge(true)
      cf.setSendAcksAsync(true)
      cf.createConnection()
    }
  )

  def topic(topicName: String, brokerUri: String): Receiver[String] = new JmsTopicReceiver(
    topicName = topicName,
    transformer = msg => msg.asInstanceOf[javax.jms.TextMessage].getText(),
    connectionProvider = () => {
      val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerUri)
      cf.setOptimizeAcknowledge(true)
      cf.setSendAcksAsync(true)
      cf.createConnection()
    }
  )

}
