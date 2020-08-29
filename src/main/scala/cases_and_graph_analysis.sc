/*
Aim: The aim of this analysis is to find the order in which the COVID-19 caused deaths across the world.
 */
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import DFHelper._
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.graphframes._
import vegas.{Line, Quant, Temp, Vegas}
import vegas.sparkExt._
import vegas._
import vegas.render.WindowRenderer._
val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate
import spark.implicits._

// input files
val data_dir = "C:/Users/ac09983/Documents/Projects/covid/covid19/data/Big Data/new data/"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read files
case class Cases(CC: String, Daten: java.sql.Timestamp, Country: String, Confirmed: Double, Deaths: Double, Recovered: Double, WHOCases: Double, Population: Int)
var cases_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_cases)
cases_df = castColumnTo(cases_df, "Daten", DateType)
cases_df = castColumnTo(cases_df, "Confirmed", IntegerType)
cases_df = castColumnTo(cases_df, "Deaths", IntegerType)
cases_df = castColumnTo(cases_df, "Recovered", IntegerType)
cases_df = castColumnTo(cases_df, "WHOCases", IntegerType)
cases_df = castColumnTo(cases_df, "Population", IntegerType)
var cases_ds: Dataset[Cases] = cases_df.as[Cases]
cases_ds = cases_ds.sort($"Population", $"Country", $"Daten".desc)
//cases_ds.cache()
cases_ds.show(10)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Analysis is to find the order in which the COVID-19 caused deaths across the world.
// DSFD - Date since first death.
val timestart2 = System.nanoTime
var first_cases_ds = cases_ds.select("Country", "Daten", "Deaths").groupBy("Country", "Daten").agg(sum($"Deaths").as("Deaths"))
first_cases_ds = first_cases_ds.filter($"Deaths" > 0).sort("Daten")
first_cases_ds.sort("Daten").select("Country", "Daten")
val w1 = Window.partitionBy($"Country").orderBy($"Daten")
first_cases_ds = first_cases_ds.withColumn("rn", row_number.over(w1)).where($"rn" === 1).drop("rn")
var first_death_date = first_cases_ds.groupBy().agg(min("Daten")).select("min(Daten)").collectAsList().get(0).get(0)
first_cases_ds = first_cases_ds.withColumn("DSFD", datediff($"Daten", lit(first_death_date)))
first_cases_ds.sort("DSFD").show(100)
val duration_dsfd = (System.nanoTime - timestart2) / 1e9d
println("Time taken for dsfd:", duration_dsfd)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Which country's health care is performing worse?
// List of countries sorted based on the death rate
// Death Rate vs Recovery rate for different countries
val timestart1 = System.nanoTime
var recovery_deathrate_ds = cases_ds.withColumn("RecoveryRate", when($"Confirmed".equalTo(0), 0).otherwise($"Recovered"/$"Confirmed"))
recovery_deathrate_ds = recovery_deathrate_ds.withColumn("DeathRate", when($"Confirmed".equalTo(0), 0).otherwise($"Deaths"/$"Confirmed"))
recovery_deathrate_ds = recovery_deathrate_ds.select("Country", "Daten", "Confirmed", "Recovered", "Deaths", "RecoveryRate", "DeathRate")
recovery_deathrate_ds = recovery_deathrate_ds.sort($"Country", $"Daten")
recovery_deathrate_ds.cache()
recovery_deathrate_ds.show(100)
val w2 = Window.partitionBy($"Country").orderBy($"Daten".desc)
var recovery_deathrate_lastcases_ds = recovery_deathrate_ds.withColumn("rn", row_number.over(w2)).where($"rn" === 1).drop("rn")
val death_rate_ds = recovery_deathrate_lastcases_ds.select("Country", "Confirmed", "Deaths", "DeathRate").filter($"Confirmed" > 10000).sort($"DeathRate".desc)
death_rate_ds.cache()
death_rate_ds.show(20)
val recovery_rate_ds = recovery_deathrate_lastcases_ds.select("Country", "Confirmed", "Recovered", "RecoveryRate").filter($"Confirmed" > 10000).sort($"RecoveryRate".desc)
recovery_rate_ds.cache()
recovery_rate_ds.show(20)
val duration_recovery_deathrate = (System.nanoTime - timestart1) / 1e9d
println("Time taken for recovery rate/death rate calculation:", duration_recovery_deathrate)
var global_survival_death = recovery_deathrate_ds.groupBy("Daten").agg(sum($"Confirmed").as("Confirmed"), sum($"Recovered").as("Recovered"), sum($"Deaths").as("Deaths"))
global_survival_death = global_survival_death.withColumn("DeathRate", when($"Confirmed".equalTo(0), 0).otherwise($"Deaths"/$"Confirmed"))
global_survival_death = global_survival_death.withColumn("RecoveryRate", when($"Confirmed".equalTo(0), 0).otherwise($"Recovered"/$"Confirmed"))
global_survival_death.cache()
val plot1 = Vegas("Country Recovery Rate").
  withDataFrame(global_survival_death.select("Daten", "DeathRate")).
  encodeX("Daten", Temp).
  encodeY("DeathRate", Quant).
  mark(Line).show
val plot2 = Vegas("Country Death Rate").
  withDataFrame(global_survival_death).
  encodeX("Daten", Temp).
  encodeY("DeathRate", Quant).
  mark(Line).show

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Distance based analysis - graphx, number of infections per 1000
// take last day's number of infections
// create a graph of countries
// when selecting a country - list nearby countries
// find the number of infections per 1000 of those nearby countries
val country = "Luxembourg"
val threshold_population = 500000
val w3 = Window.partitionBy($"Country").orderBy($"Daten".desc)
var lastcases_ds = cases_ds.withColumn("rn", row_number.over(w3)).where($"rn" === 1).drop("rn")
//lastcases_ds = lastcases_ds.filter($"Population" > threshold_population)
var country_combinations = lastcases_ds.select("Country", "Lat", "Long").withColumnRenamed("Country", "Country1").withColumnRenamed("Lat", "Lat1").withColumnRenamed("Long", "Long1").crossJoin(lastcases_ds.select("Country", "Lat", "Long")).withColumnRenamed("Country", "Country2").withColumnRenamed("Lat", "Lat2").withColumnRenamed("Long", "Long2")
country_combinations = country_combinations.filter($"Country1" =!= $"Country2")
def Distance = udf((lat1: Double, long1: Double, lat2: Double, long2: Double) => {
  val loc1 = DFHelper.Location(lat1, long1)
  val loc2 = DFHelper.Location(lat2, long2)
  new DistanceCalculatorImpl().calculateDistanceInKilometer(loc1, loc2)
} )
country_combinations = country_combinations.withColumn("Distance", Distance($"Lat1", $"Long1", $"Lat2", $"Long2"))
country_combinations = country_combinations.filter($"Country1" =!= $"Country2")
country_combinations = country_combinations.filter($"Distance" < 700)
country_combinations = country_combinations.filter($"Distance" > 0)
country_combinations.select("Country1", "Country2", "Distance").coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(data_dir + "Country_Edge")
//country_combinations.cache()
country_combinations.filter($"Country1" === "Luxembourg").sort($"Distance".asc).show()
var country_combinations_new: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(data_dir + "Country_Edge")
var edge_ip = country_combinations_new.select("Country1", "Country2", "Distance").withColumnRenamed("Country1", "src").withColumnRenamed("Country2", "dst").withColumnRenamed("Distance", "relationship").toDF()
var country_distance_gf = GraphFrame.fromEdges(edge_ip)
//val results = country_distance_gf.pageRank.resetProbability(0.15).maxIter(10).run()
var paths = country_distance_gf.shortestPaths.landmarks(Seq(country)).run()
paths = paths.select($"id", map_values($"distances")).filter(size($"map_values(distances)") > 0).select($"id", $"map_values(distances)".getItem(0))
paths.cache()
println("Directly impacting countries: ")
var shortest_path_countries = paths.filter($"map_values(distances)[0]" <= 2).sort($"map_values(distances)[0]".asc)
shortest_path_countries = shortest_path_countries.withColumnRenamed("id", "Country")
lastcases_ds = lastcases_ds.withColumn("NumInfectionsPer1000", $"Confirmed"*1000/$"Population")
shortest_path_countries = shortest_path_countries.join(lastcases_ds.select("Country", "NumInfectionsPer1000"), Seq("Country"), "left").sort($"NumInfectionsPer1000".desc)
shortest_path_countries.show(100)

// page rank with graph x
var country_distane_gx = country_distance_gf.toGraphX
val country_ranks = country_distane_gx.pageRank(0.0001, 0.15).vertices
val country_ranks_ranksort = country_ranks.sortBy(_._2, ascending=false)
val country_ranks_vertexsort = country_ranks.sortBy(_._1, ascending=true)
country_ranks_ranksort.take(10).foreach(tup => println(tup._1 + " has rank (pagerank): " + tup._2 + "."))
country_ranks_vertexsort.take(10).foreach(tup => println(tup._1 + " has rank (pagerank): " + tup._2 + "."))
