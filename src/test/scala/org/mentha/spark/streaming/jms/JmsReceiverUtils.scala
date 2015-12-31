package org.mentha.spark.streaming.jms

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
