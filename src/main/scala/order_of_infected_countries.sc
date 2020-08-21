/*
Aim: The aim of this analysis is to find the order in which the COVID-19 caused deaths across the world.
 */
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import DFHelper._
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.types.{DateType, IntegerType}

// udf - helper functions
val days_since_nearest_holidays = udf(
  (year:String, month:String, dayOfMonth:String) => year.toInt + 27 + month.toInt-12
)


import scala.collection.mutable.ArrayBuffer
val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate
import spark.implicits._

// input files
val data_dir = "C:/Users/ac09983/Documents/Projects/covid/covid19/data/Big Data/big data project/"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read files
case class Cases(CC: String, Daten: java.sql.Timestamp, Country: String, Lat: String, Confirmed: Double, Deaths: Double, Recovered: Double, WHOCases: Double)
var cases_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_cases)
cases_df = castColumnTo(cases_df, "Daten", DateType)
cases_df = castColumnTo(cases_df, "Confirmed", IntegerType)
cases_df = castColumnTo(cases_df, "Deaths", IntegerType)
cases_df = castColumnTo(cases_df, "Recovered", IntegerType)
cases_df = castColumnTo(cases_df, "WHOCases", IntegerType)
var cases_ds: Dataset[Cases] = cases_df.as[Cases]
var first_cases_ds = cases_ds.select("Country", "Daten", "Deaths").groupBy("Country", "Daten").sum("Deaths").withColumnRenamed("Sum(Deaths)", "Deaths")
first_cases_ds = first_cases_ds.filter($"Deaths" > 0).sort("Daten")
first_cases_ds.sort("Daten").select("Country", "Daten")
val w = Window.partitionBy($"Country").orderBy($"Daten")
first_cases_ds = first_cases_ds.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
var first_death_date = first_cases_ds.groupBy().agg(min("Daten")).select("min(Daten)").collectAsList().get(0).get(0)
first_cases_ds = first_cases_ds.withColumn("DSFD", datediff($"Daten", lit(first_death_date)))
first_cases_ds.sort("DSFD").show(100)
