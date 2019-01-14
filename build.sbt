name := "spark_demo"
version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0" exclude ("com.fasterxml.jackson.core", "jackson-core")

libraryDependencies += "org.apache.hbase" % "hbase" % "2.0.3" pomOnly()
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.3"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.3"
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "2.0.3"
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.0.3"



dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


