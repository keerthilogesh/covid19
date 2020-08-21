import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate

import spark.implicits._

// input files
val data_dir = "C:/Users/ac09983/Documents/Projects/covid/covid19/data/Big Data/big data project/"
val inputFile_mobility = data_dir + "Global_Mobility_Report.csv"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read files
var mobility_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_mobility)
var cases_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_cases)

// data transformation
mobility_df = mobility_df.withColumnRenamed("country_region", "Country")
mobility_df = mobility_df.withColumnRenamed("country_region_code", "CC")
mobility_df = mobility_df.withColumn("Date", to_date(col("date")))
mobility_df = mobility_df.withColumn("mobility", col("retail_and_recreation_percent_change_from_baseline") + col("grocery_and_pharmacy_percent_change_from_baseline") + col("parks_percent_change_from_baseline") + col("transit_stations_percent_change_from_baseline") + col("workplaces_percent_change_from_baseline") + col("residential_percent_change_from_baseline"))
mobility_df = mobility_df.groupBy("CC", "Country", "Date").mean("mobility")
mobility_df = mobility_df.sort("Country", "Date")

// single country analysis
val cc = "DE"
mobility_df = mobility_df.filter($"CC" === cc)

