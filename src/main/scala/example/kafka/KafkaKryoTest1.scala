package example.kafka

import java.util.Properties

import com.twitter.chill.KryoInjection
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.Random

/**
 * Created by cookeem on 16/2/25.
 */
case class Person(name: String, age: Int)

object KafkaKryoTest1 extends App {
  val person = new Person("cookeem", 37)

  val prompt = "Usage: example.kafka.KafkaKryoTest1 <topic> <groupname>"
  var topic = ""
  var groupname = ""
  var err = 1
  if (args.length == 2) {
    topic = args(0)
    groupname = args(1)
    err = 0
  }
  if (err == 1) {
    println(prompt)
  } else {
    KafkaTest1.createTopic("localhost:2181", topic, 3, 2)
    partitionKryoProducer("localhost:9092,localhost:9093,localhost:9094", topic, person)
    Thread.sleep(1000)
    simpleKryoConsumer("localhost:2181", topic, groupname)
    Thread.sleep(1000)
  }

  //通过api向分区topic写kryo数据
  def partitionKryoProducer(brokers: String, topic: String, person: Person) = {
    val propsProcducer = new Properties()
    propsProcducer.put("metadata.broker.list", brokers)
    propsProcducer.put("key.serializer.class", "kafka.serializer.StringEncoder")
    propsProcducer.put("serializer.class", "kafka.serializer.DefaultEncoder")
    propsProcducer.put("partitioner.class", "example.kafka.SimplePartitioner")
    propsProcducer.put("request.required.acks", "1")
    //producer需要使用[String, String]情况下,才能够调用SimplePartitioner
    val producer = new Producer[String, Array[Byte]](new ProducerConfig(propsProcducer))
    (1 to 3).foreach(i => {
      val key = Random.nextInt(3).toString
      val bytes = KryoInjection(person)
      producer.send(new KeyedMessage[String, Array[Byte]](topic, key, bytes))
      println(s"producer send $person[$key]")
    })
    producer.close
    println(s"producer close!")
  }

  //kafka的kryo序列化consumer
  def simpleKryoConsumer(zkUri: String, topic: String, groupname: String) = {
    val propsConsumer = new Properties()
    propsConsumer.put("group.id", groupname)
    propsConsumer.put("zookeeper.connect", zkUri)
    propsConsumer.put("zookeeper.session.timeout.ms", "4000")
    propsConsumer.put("zookeeper.sync.time.ms", "200")
    propsConsumer.put("auto.commit.interval.ms", "1000")
    propsConsumer.put("auto.offset.reset", "smallest")
    val consumerConfig = new ConsumerConfig(propsConsumer)
    val consumer = Consumer.create(consumerConfig)
    val topicCountMap = Map[String, Int](topic -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val stream = consumerMap.get(topic).get(0)
    val it = stream.iterator()
    while (it.hasNext()) {
      val item = it.next()
      val person = KryoInjection.invert(item.message()).getOrElse(null)
      println(s"consume: partition:[${item.partition}] person:[$person]")
    }
    consumer.shutdown()
    println(s"consumer shutdown!")
  }
}
