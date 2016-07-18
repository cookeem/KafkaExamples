name := "KafkaExamples"

version := "1.0"

scalaVersion := "2.11.8"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= {
	val akkaV = "2.4.8"
	Seq(
		"org.apache.kafka" %% "kafka" % "0.10.0.0",
		"com.twitter" %% "chill-bijection" % "0.8.0",
		"com.esotericsoftware" % "kryo" % "3.0.3",
		"com.typesafe.akka" %%  "akka-actor"  % akkaV,
		"com.typesafe.akka" %% "akka-stream" % akkaV,
		"com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M4",
		"org.slf4j" % "slf4j-simple" % "1.7.10",
		"org.scala-lang.modules" %% "scala-async" % "0.9.5"
	)
}
