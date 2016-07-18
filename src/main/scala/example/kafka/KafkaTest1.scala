package example.kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.javaapi.producer.Producer
import kafka.producer.{Partitioner, KeyedMessage, ProducerConfig}
import kafka.consumer.{ConsumerConfig, Consumer}
import kafka.utils.{VerifiableProperties, ZkUtils}

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Created by cookeem on 16/2/22.
 */

object KafkaTest1 extends App {
  var mode = "producer"
  var topic = ""
  var content = ""
  var groupname = ""
  val prompt =
    """Usage: example.kafka.KafkaTest1
    producer <topic> <content> |
    consumer <topic> <groupname> |
    listtopic |
    createtopic <topic> |
    deletetopic <topic> |
    partprod <topic> <content>"""
  var err = 1
  if (args.length > 0) {
    mode = args(0)
  }
  if (mode == "producer" && args.length == 3) {
    topic = args(1)
    content = args(2)
    err = 0
  }
  if (mode == "consumer" && args.length == 3) {
    topic = args(1)
    groupname = args(2)
    err = 0
  }
  if (mode == "listtopic" && args.length == 1) {
    err = 0
  }
  if (mode == "createtopic" && args.length == 2) {
    topic = args(1)
    err = 0
  }
  if (mode == "deletetopic" && args.length == 2) {
    topic = args(1)
    err = 0
  }
  if (mode == "partprod" && args.length == 3) {
    topic = args(1)
    content = args(2)
    err = 0
  }
  if (err == 1) {
    println(prompt)
  } else {
    if (mode == "producer") {
      simpleProducer("localhost:9092,localhost:9093,localhost:9094", topic, content)
    } else if (mode == "consumer") {
      simpleConsumer("localhost:2181", topic, groupname)
    } else if (mode == "listtopic") {
      listTopic("localhost:2181")
    } else if (mode == "createtopic") {
      createTopic("localhost:2181", topic, numPartitions = 3, replicationFactor = 2)
    } else if (mode == "deletetopic") {
      deleteTopic("localhost:2181", topic)
    } else if (mode == "partprod") {
      partitionProducer("localhost:9092,localhost:9093,localhost:9094", topic, content)
    }
  }

  //简单producer输入文本到kafka
  def simpleProducer(brokers: String, topic: String, content: String) = {
    val propsProcducer = new Properties()
    propsProcducer.put("serializer.class", "kafka.serializer.StringEncoder")
    propsProcducer.put("metadata.broker.list", brokers)
    val producer = new Producer[Int, String](new ProducerConfig(propsProcducer))
    producer.send(new KeyedMessage[Int, String](topic, content))
    println(s"producer send $content")
    producer.close
    println(s"producer close!")

  }

  //kafka的consumer
  def simpleConsumer(zkUri: String, topic: String, groupname: String) = {
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
      println(s"consume: partition:[${item.partition}] message:[${new String(item.message)}]")
    }
    consumer.shutdown()
    println(s"consumer shutdown!")
  }

  //通过api列表topic
  def listTopic(zkUri: String) = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUri, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
    val topics = AdminUtils.fetchAllTopicConfigs(zkUtils).map{case (topicName, topicProps) => topicName}.toSet
    val topicsMeta = AdminUtils.fetchTopicMetadataFromZk(topics,zkUtils)
    topicsMeta.foreach{tm =>
      val pmd = tm.partitionMetadata().map{pm =>
        s"partitionId:[${pm.partition()}] leader:[${pm.leader}] isr:[${pm.isr}] replicas:[${pm.replicas}]"
      }.mkString("\n")
      println(s"### topic: [${tm.topic}] ")
      println(s"### partitionsProps:")
      println(s"[$pmd]")
    }
  }

  //通过api创建topic
  def createTopic(zkUri: String, topic: String, numPartitions: Int, replicationFactor: Int) = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUri, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
    val topicConfig = new Properties()
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
      println(s"create kafka topic $topic")
    } else {
      println(s"kafka topic $topic already exists")
    }
  }

  //通过api删除topic
  def deleteTopic(zkUri: String, topic: String) = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUri, sessionTimeoutMs, connectionTimeoutMs)
    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
    if (AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.deleteTopic(zkUtils, topic)
      zkClient.deleteRecursive(s"/brokers/topics/$topic")
      println(s"delete kafka topic $topic")
    } else {
      println(s"kafka topic $topic not exists")
    }
  }

  //通过api向分区topic写数据
  def partitionProducer(brokers: String, topic: String, content: String) = {
    val propsProcducer = new Properties()
    propsProcducer.put("metadata.broker.list", brokers)
    propsProcducer.put("serializer.class", "kafka.serializer.StringEncoder")
    propsProcducer.put("partitioner.class", "example.kafka.SimplePartitioner")
    propsProcducer.put("request.required.acks", "1")
    //producer需要使用[String, String]情况下,才能够调用SimplePartitioner
    val producer = new Producer[String, String](new ProducerConfig(propsProcducer))
    (1 to 3).foreach(i => {
      val key = Random.nextInt(3).toString
      val newContent = content + "-" + i
      producer.send(new KeyedMessage[String, String](topic, key, newContent))
      println(s"producer send $newContent[$key]")
    })
    producer.close
    println(s"producer close!")
  }
}

class SimplePartitioner(var props: VerifiableProperties) extends Partitioner {
  override def partition(key: scala.Any, numPartitions: Int) = {
    var partition = 0
    partition = Math.abs(key.hashCode()) % numPartitions
    println(s"###SimplePartitioner: $partition")
    partition
  }
}
