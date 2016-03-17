name := "KafkaExamples"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
	"org.apache.kafka" %% "kafka" % "0.9.0.1",
	"com.twitter" %% "chill-bijection" % "0.8.0",
	"com.esotericsoftware" % "kryo" % "3.0.3"
)
