name := "IDSStreaming"
version := "1.0"
scalaVersion := "2.12.15"

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % "3.6.0",
  // Added for MinIO/S3 support
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)