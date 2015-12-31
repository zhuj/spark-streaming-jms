# spark-streaming-jms
Simple JMS Receiver for [Apache Spark Streaming](http://spark.apache.org/streaming/).

Usage example:
```
 val sc: SparkContext = SparkContext.getOrCreate(conf)
 val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
 
 val brokerURL = "tcp://localhost:61616"
 val username = "admin"
 val password = "admin"
 val queueName = "testQueue"
 
 val stream: InputDStream[String] = ssc.receiverStream(new JmsReceiver(
   queueName = queueName,
   transformer = { msg => msg.asInstanceOf[javax.jms.TextMessage].getText() },
   connectionProvider = { () => {
     val cf = new org.apache.activemq.ActiveMQConnectionFactory(brokerURL)
     cf.setOptimizeAcknowledge(true)
     cf.setSendAcksAsync(true)
     cf.createConnection(username, password)
   }}
 ))
 
 ...
 
 ssc.start()
 ssc.awaitTermination()
```
