import DFHelper.castColumnTo
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

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

// How mobility reduction helps in tackling the spread of infections
val cc = "DE"
var mobility_ds_country = mobility_ds.filter($"CC" === cc)
var cases_ds_country = cases_ds.filter($"CC" === cc)
var cases_mobility_ds = cases_ds_country.join(mobility_ds_country.select("CC", "Date", "mobility"), Seq("CC", "Date"), "left")
cases_mobility_ds = cases_mobility_ds.na.fill(0, Seq("mobility"))
cases_mobility_ds.cache()
val window_week =  Window.partitionBy("CC", "Country").orderBy(asc("Date")).rowsBetween(-6, 0)
var weekly_cases_mobility_ds = cases_mobility_ds.select($"CC", $"Country", $"Date", $"mobility", avg($"mobility").over(window_week).as("mobility_week"), $"Confirmed", sum($"Confirmed").over(window_week).as("Confirmed_week"))
weekly_cases_mobility_ds.show(10)
val corr_inst_week = weekly_cases_mobility_ds.stat.corr("mobility_week", "Confirmed_week")
val corr_inst = weekly_cases_mobility_ds.stat.corr("mobility", "Confirmed")
println("Instantaneous correlation between mobility and confirmed cases: ", corr_inst)
println("Instantaneous correlation between weekly mobility and confirmed cases: ", corr_inst_week)

val formula = new RFormula()
  .setFormula("Confirmed ~ mobility_week + mobility")
  .setFeaturesCol("features")
  .setLabelCol("label")

val output = formula.fit(weekly_cases_mobility_ds.select("mobility", "mobility_week", "Confirmed")).transform(weekly_cases_mobility_ds.select("mobility", "mobility_week", "Confirmed"))
output.select("features", "label").distinct().show(100)
val chi = ChiSquareTest.test(output, "features", "label").head
println("pValues = " + chi.getAs[Vector](0))
println("degreesOfFreedom = " + chi.getSeq[Int](1).mkString("[", ",", "]"))
println("statistics = " + chi.getAs[Vector](2))
