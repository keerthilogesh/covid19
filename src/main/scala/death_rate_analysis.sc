/*
Aim: The aim of this analysis is to find the order in which the COVID-19 caused deaths across the world.
 */
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import DFHelper._
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.graphx._
import org.graphframes._

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
val inputFile_mobility = data_dir + "Global_Mobility_Report.csv"

// read files
case class Cases(CC: String, Daten: java.sql.Timestamp, Country: String, Lat: String, Confirmed: Double, Deaths: Double, Recovered: Double, WHOCases: Double)
var cases_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_cases)
cases_df = castColumnTo(cases_df, "Daten", DateType)
cases_df = castColumnTo(cases_df, "Confirmed", IntegerType)
cases_df = castColumnTo(cases_df, "Deaths", IntegerType)
cases_df = castColumnTo(cases_df, "Recovered", IntegerType)
cases_df = castColumnTo(cases_df, "WHOCases", IntegerType)
var cases_ds: Dataset[Cases] = cases_df.as[Cases]

// Which country's health care is performing worse?
var death_rate_ds = cases_ds.select("Country", "Daten", "Deaths", "Confirmed").groupBy("Country", "Daten").sum("Deaths", "Confirmed").withColumnRenamed("Sum(Deaths)", "Deaths").withColumnRenamed("Sum(Confirmed)", "Confirmed")
death_rate_ds = death_rate_ds.withColumn("DeathRate", $"Deaths"/$"Confirmed")
death_rate_ds = death_rate_ds.na.fill(0.0).sort("Country", "Daten")
death_rate_ds = death_rate_ds.groupBy("Country").mean("DeathRate").sort($"avg(DeathRate)".desc)
death_rate_ds.show(100)

// Distance based analysis - graphx, number of infections per 1000
// take last day's number of infections
// create a graph of countries
// when selecting a country - list nearby countries
// find the number of infections per 1000 of those nearby countries
val country = "Spain"
val threshold_population = 200000
val w = Window.partitionBy($"Country").orderBy($"Daten".desc)
var cases_ds_filtered = cases_ds.filter($"Population" > threshold_population)
var last_cases_ds = cases_ds_filtered.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
var country_combinations = last_cases_ds.select("Country", "Lat", "Long").withColumnRenamed("Country", "Country1").withColumnRenamed("Lat", "Lat1").withColumnRenamed("Long", "Long1").crossJoin(last_cases_ds.select("Country", "Lat", "Long")).withColumnRenamed("Country", "Country2").withColumnRenamed("Lat", "Lat2").withColumnRenamed("Long", "Long2")
country_combinations = country_combinations.filter($"Country1" =!= $"Country2")
def Distance = udf((lat1: Double, long1: Double, lat2: Double, long2: Double) => {
  val loc1 = DFHelper.Location(lat1, long1)
  val loc2 = DFHelper.Location(lat2, long2)
  new DistanceCalculatorImpl().calculateDistanceInKilometer(loc1, loc2)
} )
country_combinations = country_combinations.withColumn("Distance", Distance($"Lat1", $"Long1", $"Lat2", $"Long2"))
country_combinations = country_combinations.filter($"Distance" < 1000)
country_combinations = country_combinations.filter($"Country1" =!= $"Country2")
country_combinations = country_combinations.filter($"Distance" > 10)
country_combinations.select("Country1", "Country2", "Distance").coalesce(1).write.option("header", "false").option("sep", ",").mode("overwrite").csv(data_dir + "Country_Edge")
var edge_ip = country_combinations.select("Country1", "Country2", "Distance").withColumnRenamed("Country1", "src").withColumnRenamed("Country2", "dst").withColumnRenamed("Distance", "relationship").toDF()
var country_distance_gf = GraphFrame.fromEdges(edge_ip)
val results = country_distance_gf.pageRank.resetProbability(0.15).maxIter(10).run()
var country_distane_gx = country_distance_gf.toGraphX
var paths = country_distance_gf.shortestPaths.landmarks(Seq(country)).run()
paths = paths.select($"id", map_values($"distances")).filter(size($"map_values(distances)") > 0).select($"id", $"map_values(distances)".getItem(0))
// directly impacting countries
println("Directly impacting countries: ")
paths.filter($"map_values(distances)[0]" <= 1).show(100)
// first order indirectly impacting countries
paths.filter($"map_values(distances)[0]" === 2).show(100)
// second order indirectly impacting countries
paths.filter($"map_values(distances)[0]" === 3).show(100)
paths.filter($"map_values(distances)[0]" === 4).show(100)
paths.filter($"map_values(distances)[0]" > 10).show(100)
// severity check - number of infections per 1000 persons in the country
last_cases_ds = last_cases_ds.withColumn("NumInfectionsPer1000", $"Confirmed"*1000/$"Population")
last_cases_ds.sort($"NumInfectionsPer1000".desc).show()

