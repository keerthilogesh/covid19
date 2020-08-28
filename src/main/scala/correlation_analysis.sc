import DFHelper.castColumnTo
import org.apache.spark.ml.feature.{RFormula, FeatureHasher, VectorAssembler}
import org.apache.spark.ml.stat._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.ml.linalg.{Vector, Vectors, Matrix}
import vegas._

import scala.collection.mutable.ArrayBuffer

val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate

import spark.implicits._

// input files
val data_dir = "C:/Users/ac09983/Documents/Projects/covid/covid19/data/Big Data/big data project/"
val inputFile_mobility = data_dir + "Global_Mobility_Report.csv"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read files
var cases_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_cases)
case class Cases(CC: String, Date: java.sql.Timestamp, Country: String, Confirmed: Double, Deaths: Double, Recovered: Double, WHOCases: Double)
cases_df = castColumnTo(cases_df, "Daten", DateType)
cases_df = castColumnTo(cases_df, "Confirmed", IntegerType)
cases_df = castColumnTo(cases_df, "Deaths", IntegerType)
cases_df = castColumnTo(cases_df, "Recovered", IntegerType)
cases_df = castColumnTo(cases_df, "WHOCases", IntegerType)
cases_df = cases_df.withColumnRenamed("Daten", "Date")
cases_df = cases_df.drop("Lat", "Long", "Population")
var cases_ds: Dataset[Cases] = cases_df.as[Cases]
var mobility_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_mobility)
mobility_df = mobility_df.withColumnRenamed("country_region", "Country")
mobility_df = mobility_df.withColumnRenamed("country_region_code", "CC")
mobility_df = mobility_df.withColumn("Date", to_date(col("date")))
mobility_df = mobility_df.withColumn("mobility", col("retail_and_recreation_percent_change_from_baseline") + col("grocery_and_pharmacy_percent_change_from_baseline") + col("parks_percent_change_from_baseline") + col("transit_stations_percent_change_from_baseline") + col("workplaces_percent_change_from_baseline") + col("residential_percent_change_from_baseline"))
mobility_df = mobility_df.groupBy("CC", "Country", "Date").agg(mean($"mobility").as("mobility"))
mobility_df = mobility_df.sort("Country", "Date")
case class Mobility(CC: String, Date: java.sql.Timestamp, Country: String, mobility: Double)
var mobility_ds = mobility_df.as[Mobility]

// Add moving window of past week and month stats
val cc = "DE"
var mobility_ds_country = mobility_ds.filter($"CC" === cc)
var cases_ds_country = cases_ds.filter($"CC" === cc)
var cases_mobility_ds = cases_ds_country.join(mobility_ds_country.select("CC", "Date", "mobility"), Seq("CC", "Date"), "left")
cases_mobility_ds = cases_mobility_ds.na.fill(0, Seq("mobility"))
cases_mobility_ds.cache()
val window_week =  Window.partitionBy("CC", "Country").orderBy(asc("Date")).rowsBetween(-6, 0)
val window_month =  Window.partitionBy("CC", "Country").orderBy(asc("Date")).rowsBetween(-29, 0)
val window_diff = Window.partitionBy("CC", "Country").orderBy(asc("Date"))
cases_mobility_ds = cases_mobility_ds.select($"CC", $"Country", $"Date", $"mobility", $"Deaths", $"Confirmed", $"Recovered", $"WHOCases",
  avg($"mobility").over(window_week).as("mobility_week"),
  avg($"mobility").over(window_month).as("mobility_month"),
  sum($"Deaths").over(window_week).as("Deaths_week"),
  sum($"Deaths").over(window_month).as("Deaths_month"),
  sum($"Recovered").over(window_week).as("Recovered_week"),
  sum($"Recovered").over(window_month).as("Recovered_month"),
  sum($"WHOCases").over(window_week).as("WHOCases_week"),
  sum($"WHOCases").over(window_month).as("WHOCases_month"),
  sum($"Confirmed").over(window_week).as("Confirmed_month"),
  sum($"Confirmed").over(window_month).as("Confirmed_week"))
cases_mobility_ds = cases_mobility_ds.withColumn("mobility_change", $"mobility" - lag("mobility", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("Deaths_change", $"Deaths" - lag("Deaths", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("Recovered_change", $"Recovered" - lag("Recovered", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("Confirmed_change", $"Confirmed" - lag("Confirmed", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("WHOCases_change", $"WHOCases" - lag("WHOCases", 1).over(window_diff))
cases_mobility_ds.show(100)

// VectorAssembler is a transformer that combines a given list of columns into a single vector column.
cases_mobility_ds = cases_mobility_ds.na.fill(0.0, Array("Confirmed", "Recovered", "WHOCases", "mobility", "Deaths",
  "Confirmed_week", "Recovered_week", "WHOCases_week", "mobility_week", "Deaths_week",
  "Confirmed_month", "Recovered_month", "WHOCases_month", "mobility_month", "Deaths_month",
  "Confirmed_change", "Recovered_change", "WHOCases_change", "mobility_change", "Deaths_change"))
val assembler_confirmed = new VectorAssembler()
  .setInputCols(Array("Confirmed_month", "Confirmed_week", "Confirmed_change", "mobility", "mobility_week", "mobility_month"))
  .setOutputCol("features")
val assembler_deaths = new VectorAssembler()
  .setInputCols(Array("Deaths_month", "Deaths_week", "Deaths_change", "mobility", "mobility_week", "mobility_month"))
  .setOutputCol("features")
val vector_confirmed = assembler_confirmed.transform(cases_mobility_ds)
val vector_deaths = assembler_deaths.transform(cases_mobility_ds)
val Row(coeff11: Matrix) = Correlation.corr(vector_confirmed, "features").head
println("Pearson correlation matrix:\n" + coeff11.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff21: Matrix) = Correlation.corr(vector_confirmed, "features", "spearman").head
println("spearman correlation matrix:\n" + coeff21.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff12: Matrix) = Correlation.corr(vector_deaths, "features").head
println("Pearson correlation matrix:\n" + coeff12.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff22: Matrix) = Correlation.corr(vector_deaths, "features", "spearman").head
println("spearman correlation matrix:\n" + coeff22.toString(Int.MaxValue, Int.MaxValue))
// mobility has strong negative correlation with the new cases and the mortality rate
// people tend to move a little when the number of cases is decreasing

// Recovery rate vs Death rate
cases_mobility_ds = cases_mobility_ds.withColumn("RecoveryRate", when($"Confirmed".equalTo(0), 0).otherwise($"Recovered"/$"Confirmed"))
cases_mobility_ds = cases_mobility_ds.withColumn("DeathRate", when($"Confirmed".equalTo(0), 0).otherwise($"Deaths"/$"Confirmed"))
var recovery_death_ds = cases_mobility_ds.select("Country", "Date", "RecoveryRate", "DeathRate")
recovery_death_ds.show(1000)

import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

val plot1 = Vegas("Country Pop").
  withDataFrame(recovery_death_ds).
  encodeX("Date", Temp).
  encodeY("RecoveryRate", Quant).
  mark(Line).show

val plot2 = Vegas("Country Pop").
  withDataFrame(recovery_death_ds).
  encodeX("Date", Temp).
  encodeY("DeathRate", Quant).
  mark(Line).show
