name := "delta-bronze-layer"

version := "0.1"

scalaVersion := "2.13.6"
scalaVersion := "2.12.11"
version := "0.1.0"

def getDeltaVersion(): String = {
  val envVars = System.getenv
  if (envVars.containsKey("DELTA_VERSION")) {
    val version = envVars.get("DELTA_VERSION")
    println("Using Delta version " + version)
    version
  } else {
    "1.0.0"
  }
}

lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo =>
  resolvers += "Delta" at repo
}

lazy val root = (project in file("."))
  .settings(
    name := "hello-world",
    libraryDependencies += "io.delta" %% "delta-core" % getDeltaVersion(),
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-yarn" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1",
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.985",
    libraryDependencies += "io.github.embeddedkafka" % "embedded-kafka_2.12" % "2.8.0",
    extraMavenRepo
  )