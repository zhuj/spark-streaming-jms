# spark-streaming-jms
Simple JMS Receiver for [Apache Spark Streaming](http://spark.apache.org/docs/1.5.2/streaming-programming-guide.html).

Usage example (see [Example.scala](src/test/scala/org/mentha/spark/streaming/jms/Example.scala) for details):
```
 case class Message(
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

  def receiver() = new JmsQueueReceiver[Message](
   queueName = queueName,
   transformer = msg => Message(
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
 
 ...
 
 val sc: SparkContext = SparkContext.getOrCreate(conf)
 val ssc: StreamingContext = new StreamingContext(sc, Seconds(...))
 
 ...
 
 val stream: DStream[Message] = ActiveMQStream.stream(ssc)
 
 ...
 
 ssc.start()
 ssc.awaitTermination()
```
