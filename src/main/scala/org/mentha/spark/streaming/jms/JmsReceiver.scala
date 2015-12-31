package org.mentha.spark.streaming.jms

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import javax.{jms => jms}

/** Simple class of a receiver that can be run on worker nodes to receive the data from JMS.
  *
  * {{{
  *  val sc: SparkContext = SparkContext.getOrCreate(conf)
  *  val scc: StreamingContext = new StreamingContext(sc, Seconds(10))
  *
  *  val brokerURL = "tcp://localhost:61616"
  *  val username = "admin"
  *  val password = "admin"
  *  val queueName = "testQueue"
  *
  *  val stream: InputDStream[String] = scc.receiverStream(new JmsReceiver(
  *    queueName = queueName,
  *    transformer = { msg => msg.asInstanceOf[javax.jms.TextMessage].getText() },
  *    connectionProvider = { () => {
  *      val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerURL)
  *      cf.setOptimizeAcknowledge(true)
  *      cf.setSendAcksAsync(true)
  *      cf.createConnection(username, password)
  *    }}
  *  ))
  *
  *  ...
  *
  *  scc.start()
  *  scc.awaitTermination()
  * }}}
  *
  * @param connectionProvider provides [[jms.Connection]] for the receiver.
  * @param transformer (pre)transforms [[jms.Message]] to appropriate class (it's required to do this before populate the result).
  * @param queueName the name of required <CODE>Queue</CODE>.
  * @param messageSelector only messages with properties matching the message selector expression are delivered.
  * @param storageLevel flags for controlling the storage of an RDD.
  * @tparam T RDD type.
  */
class JmsReceiver[T] (
  connectionProvider: (() => jms.Connection),
  transformer: (jms.Message => T),
  queueName: String,
  messageSelector: Option[String] = None,
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
) extends Receiver[T](storageLevel = storageLevel) with Logging {

  private lazy val connection: jms.Connection = {

    // init connection & error handling
    val c: jms.Connection = connectionProvider()
    c.setExceptionListener(new jms.ExceptionListener() {
      override def onException(exception: jms.JMSException): Unit = {
        process(exception)
      }
    })

    // session, queue and consumer
    val session: jms.Session = c.createSession(false, jms.Session.AUTO_ACKNOWLEDGE)
    val destination: jms.Queue = session.createQueue(queueName)
    val consumer: jms.MessageConsumer = messageSelector match {
      case Some(s) => session.createConsumer(destination, s)
      case None => session.createConsumer(destination)
    }

    // listener
    consumer.setMessageListener(new jms.MessageListener() {
      override def onMessage(message: jms.Message): Unit = {
        process(message)
      }
    })
    c
  }

  protected def process(message: jms.Message): Unit = {
    store(transformer(message))
  }

  protected def process(exception: jms.JMSException): Unit = {
    reportError(exception.getMessage, exception)
  }

  override def onStart(): Unit = {
    connection.start()
  }

  override def onStop(): Unit = {
    connection.stop()
    connection.close()
  }

}
