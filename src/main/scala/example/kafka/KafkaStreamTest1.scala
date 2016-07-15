package example.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer


/**
  * Created by cookeem on 16/7/15.
  */
object KafkaStreamTest1 extends App {
  implicit val system = ActorSystem("kafka-stream")
  implicit val materializer = ActorMaterializer()

}
