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

import javax.{jms => jms}

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/** Simple class of a receiver that can be run on worker nodes to receive the data from JMS Queue/Topic.
  *
  * {{{
  *  val sc: SparkContext = SparkContext.getOrCreate(conf)
  *  val ssc: StreamingContext = new StreamingContext(sc, Seconds(...))
  *
  *  val stream: InputDStream[String] = ssc.receiverStream(new AbstractJmsReceiver() {
  *    override protected def buildConnection(): jms.Connection = {
  *      val cf = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616")
  *      cf.setOptimizeAcknowledge(true)
  *      cf.createConnection("username", "password")
  *    }
  *    override protected def transform(msg: jms.Message): String = msg.asInstanceOf[jms.TextMessage].getText()
  *    override protected def buildDestination(session: jms.Session): jms.Destination = session.createQueue("testQueue")
  *  })
  *
  *  ...
  *
  *  ssc.start()
  *  ssc.awaitTermination()
  * }}}
  *
  * @param messageSelector only messages with properties matching the message selector expression are delivered.
  * @param storageLevel flags for controlling the storage of an RDD.
  * @tparam T RDD element type.
  */
abstract class AbstractJmsReceiver[T] (
  messageSelector: Option[String] = None,
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
) extends Receiver[T](storageLevel = storageLevel) with Logging {

  /** creates new <CODE>javax.jms.Connection</CODE> for the receiver */
  protected def buildConnection(): jms.Connection

  /** provides <CODE>javax.jms.Destination</CODE> for the receiver */
  protected def buildDestination(session: jms.Session): jms.Destination

  private lazy val connection: jms.Connection = {

    // connection & error handling
    val c: jms.Connection = buildConnection()
    c.setExceptionListener(new jms.ExceptionListener() {
      override def onException(exception: jms.JMSException): Unit = {
        process(exception)
      }
    })

    // session & destination
    val session: jms.Session = c.createSession(false, jms.Session.AUTO_ACKNOWLEDGE)
    val destination: jms.Destination = buildDestination(session)

    // consumer & listener
    val consumer: jms.MessageConsumer = messageSelector match {
      case Some(s) => session.createConsumer(destination, s)
      case None => session.createConsumer(destination)
    }
    consumer.setMessageListener(new jms.MessageListener() {
      override def onMessage(message: jms.Message): Unit = {
        process(message)
      }
    })

    c
  }

  /** (pre)transforms <CODE>javax.jms.Message</CODE> to appropriate class (it's required to do this before populate the result). */
  protected def transform(message: jms.Message): T

  protected def process(message: jms.Message): Unit = {
    store(transform(message))
  }

  protected def process(exception: jms.JMSException): Unit = {
    reportError(exception.getMessage(), exception)
  }

  override def onStart(): Unit = {
    connection.start()
  }

  override def onStop(): Unit = {
    connection.stop()
    connection.close()
  }

}
