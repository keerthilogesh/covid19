import org.apache.spark.sql.SparkSession
val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate
import spark.implicits._
import org.graphframes.GraphFrame

val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age")
// Edge DataFrame
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
)).toDF("src", "dst", "relationship")
val g = GraphFrame(v, e)
