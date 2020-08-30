name := "covid19"
version := "0.1"
scalaVersion := "2.11.9"

val sparkVersion = "2.4.6"
val vegasVersion = "0.3.11"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2",
  "graphframes" % "graphframes" % "0.8.0-spark2.4-s_2.11",
)

libraryDependencies += "org.vegas-viz" %% "vegas" % {vegasVersion}
libraryDependencies += "org.vegas-viz" %% "vegas-spark" % {vegasVersion}
libraryDependencies += "io.github.carldata" %% "timeseries" % "0.7.0"
