name := "KafkaExamples"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= {
	val akkaV = "2.4.6"
	Seq(
		"org.apache.kafka" %% "kafka" % "0.9.0.1",
		"com.twitter" %% "chill-bijection" % "0.8.0",
		"com.esotericsoftware" % "kryo" % "3.0.3",
		"com.typesafe.akka" %%  "akka-actor"  % akkaV,
		"com.typesafe.akka" %% "akka-stream" % akkaV,
		"org.scala-lang.modules" %% "scala-async" % "0.9.5"
	)
}
