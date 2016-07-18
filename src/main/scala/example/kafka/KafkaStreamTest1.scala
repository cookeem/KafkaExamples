package example.kafka

import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer, StringSerializer, ByteArraySerializer}

/**
  * Created by cookeem on 16/7/15.
  */
object KafkaStreamTest1 extends App {
  implicit val system = ActorSystem("example")
  implicit val materializer = ActorMaterializer()

  val brokers = "localhost:9092,localhost:9093,localhost:9094"
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(brokers)

  Source(1 to 100)
    .map(i => i + "-" + UUID.randomUUID().toString)
    .map(elem => new ProducerRecord[Array[Byte], String]("test", elem))
    .to(Producer.plainSink(producerSettings)).run()

  Source(1 to 100)
    .map(elem => ProducerMessage.Message(new ProducerRecord[Array[Byte], String]("test", elem.toString), elem))
    .via(Producer.flow(producerSettings))
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value} (${result.message.passThrough}")
      result
    }.runForeach(println)

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(brokers)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer.plainSource(consumerSettings, Subscriptions.topics("test")).runForeach(println)

  Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(new TopicPartition("test", 0), 100L)).runForeach(println)
}
