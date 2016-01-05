package org.mentha.spark.streaming.jms

import org.apache.activemq.broker.{BrokerService, TransportConnector}
import org.apache.spark.Logging
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.concurrent.duration._
import scala.language.postfixOps

class JmsReceiverSuite extends FunSuite with Logging with Eventually with BeforeAndAfter {

  private val persistenceDir = {
    java.nio.file.Files.createTempDirectory(
      "mentha-spark-jms-test-"
    ).toFile
  }

  private val brokerHost = "localhost"
  private val brokerPort = {
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

  private val batchDuration = Milliseconds(500)
  private val master = "local[2]"
  private val framework = this.getClass.getSimpleName

  private val queueName = "testQueue"

  private var broker: BrokerService = _
  private var connector: TransportConnector = _

  before {
    broker = new BrokerService()
    broker.setDataDirectoryFile(persistenceDir)
    connector = new TransportConnector()
    connector.setName("openwire")
    connector.setUri(brokerUri)
    broker.addConnector(connector)
    broker.start()
  }

  after {
    if (broker != null) {
      broker.stop()
      broker = null
    }
    if (connector != null) {
      connector.stop()
      connector = null
    }
    org.apache.commons.io.FileUtils.deleteDirectory(persistenceDir)
  }

  test("jms ActiveMQ receiver") {

    val ssc = new StreamingContext(master, framework, batchDuration)
    val stream = ssc.receiverStream(JmsReceiverUtils.receiver(queueName, brokerUri.toString()))

    @volatile var receiveMessage: List[String] = List()
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        receiveMessage = receiveMessage ::: List(rdd.first)
        receiveMessage
      }
    }

    ssc.start()

    val sendMessage = "ActiveMQ Test Message"

    // Retry it because we don't know when the receiver will start.
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      publish(sendMessage)
      assert(sendMessage.equals(receiveMessage(0)))
    }

    ssc.stop()
  }

  private def publish(msg: String): Unit = {

    val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerUri)
    cf.setOptimizeAcknowledge(true)
    cf.setSendAcksAsync(true)

    val c = cf.createConnection()

    val session = c.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)
    val destination = session.createQueue(queueName)

    val producer =  session.createProducer(destination)
    c.start()

    val message = session.createTextMessage(msg)
    producer.send(message)

    c.close()
  }

}

private[jms] object JmsReceiverUtils {

  def receiver(queueName: String, brokerUri: String) = new JmsReceiver(
    queueName = queueName,
    transformer = { msg => msg.asInstanceOf[javax.jms.TextMessage].getText() },
    connectionProvider = { () => {
      val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerUri)
      cf.setOptimizeAcknowledge(true)
      cf.setSendAcksAsync(true)
      cf.createConnection()
    }
    }
  )

}
