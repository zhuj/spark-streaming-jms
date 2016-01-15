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

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import javax.{jms => jms}

/** Simple class of a receiver that can be run on worker nodes to receive the data from JMS Queue.
  *
  * A JMS Queue implements load balancer semantics.
  * A single message will be received by exactly one consumer.
  * If there are no consumers available at the time the message is sent it will be kept until a consumer is available that can process the message.
  *
  * {{{
  *  val sc: SparkContext = SparkContext.getOrCreate(conf)
  *  val ssc: StreamingContext = new StreamingContext(sc, Seconds(...))
  *
  *  val stream: InputDStream[String] = ssc.receiverStream(new JmsQueueReceiver(
  *    queueName = "testQueue",
  *    transformer = { msg => msg.asInstanceOf[javax.jms.TextMessage].getText() },
  *    connectionProvider = { () => {
  *      val cf = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616")
  *      cf.setOptimizeAcknowledge(true)
  *      cf.createConnection("username", "password")
  *    }}
  *  ))
  *
  *  ...
  *
  *  ssc.start()
  *  ssc.awaitTermination()
  * }}}
  *
  * @param connectionProvider provides <CODE>javax.jms.Connection</CODE> for the receiver.
  * @param transformer (pre)transforms <CODE>javax.jms.Message</CODE> to appropriate class (it's required to do this before populate the result).
  * @param queueName the name of required <CODE>javax.jms.Queue</CODE>.
  * @param messageSelector only messages with properties matching the message selector expression are delivered.
  * @param storageLevel flags for controlling the storage of an RDD.
  * @tparam T RDD element type.
  */
class JmsQueueReceiver[T] (
  connectionProvider: (() => jms.Connection),
  transformer: (jms.Message => T),
  queueName: String,
  messageSelector: Option[String] = None,
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
) extends AbstractJmsReceiver[T](
  messageSelector = messageSelector,
  storageLevel = storageLevel
) with Logging {

  override protected def buildConnection(): jms.Connection = connectionProvider()
  override protected def transform(message: jms.Message): T = transformer(message)
  override protected def buildDestination(session: jms.Session): jms.Destination = session.createQueue(queueName)

}
