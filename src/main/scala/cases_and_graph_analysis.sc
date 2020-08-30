/*
Aim: The aim of this analysis is to analyze based on the merged cases dataset in the previous step.
 */
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
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

def castColumnTo( df: DataFrame, cn: String, tpe: DataType) : DataFrame = {
	/*
	A function to cast the column of the dataframe to specified dtype and then return the output dataframe
	df - input dataframe
	cn - the ip column 
	tpe - the op data type
	*/
    df.withColumn( cn, df(cn).cast(tpe) )
  }

case class Location(lat: Double, lon: Double)
  trait DistanceCalcular {
    def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
  }
  class DistanceCalculatorImpl extends DistanceCalcular {
  	/*
    This is implementation which computes the distance between two latitude and longitude points
    This implementation is taken from this reference: https://dzone.com/articles/scala-calculating-distance-between-two-locations
    */
    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
      val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
      val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
      val sinLat = Math.sin(latDistance / 2)
      val sinLng = Math.sin(lngDistance / 2)
      val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(userLocation.lat)) *
          Math.cos(Math.toRadians(warehouseLocation.lat)) *
          sinLng * sinLng)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
    }
}

// input files
// val data_dir = "C:/Users/keerthi/Documents/Projects/covid/covid19/data/Big Data/new data/"
val data_dir = "/home/users/vthamilselvan/keerthi/project/"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read master data as dataframe and cast to dataset
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

//////////////////////////////////////////////////// Analysis 2 ///////////////////////////////////////////////// 
// Analysis is to find the order in which the COVID-19 caused deaths across the world.
// DSFD - Date since first death.
val timestart_datesincefirstdeath = System.nanoTime
var first_cases_ds = cases_ds.select("Country", "Daten", "Deaths").groupBy("Country", "Daten").agg(sum($"Deaths").as("Deaths"))
first_cases_ds = first_cases_ds.filter($"Deaths" > 0).sort("Daten")
first_cases_ds.sort("Daten").select("Country", "Daten")
val w1 = Window.partitionBy($"Country").orderBy($"Daten")
first_cases_ds = first_cases_ds.withColumn("rn", row_number.over(w1)).where($"rn" === 1).drop("rn") // take the first row of the window sorted by Date
var first_death_date = first_cases_ds.groupBy().agg(min("Daten")).select("min(Daten)").collectAsList().get(0).get(0)
first_cases_ds = first_cases_ds.withColumn("DSFD", datediff($"Daten", lit(first_death_date)))
first_cases_ds.sort("DSFD").show(100)
val duration_dsfd = (System.nanoTime - timestart_datesincefirstdeath) / 1e9d
println("Time taken for dsfd:", duration_dsfd)
// duration in cluster: 20 seconds

//////////////////////////////////////////////////// Analysis 3 ///////////////////////////////////////////////// 
// Which country's health care is performing worse?
// List of countries sorted based on the death rate
// Death Rate vs Recovery rate for different countries
val timestart_recoveryrate = System.nanoTime
var recovery_deathrate_ds = cases_ds.withColumn("RecoveryRate", when($"Confirmed".equalTo(0), 0).otherwise($"Recovered"/$"Confirmed"))
recovery_deathrate_ds = recovery_deathrate_ds.withColumn("DeathRate", when($"Confirmed".equalTo(0), 0).otherwise($"Deaths"/$"Confirmed"))
recovery_deathrate_ds = recovery_deathrate_ds.select("Country", "Daten", "Confirmed", "Recovered", "Deaths", "RecoveryRate", "DeathRate")
recovery_deathrate_ds = recovery_deathrate_ds.sort($"Country", $"Daten")
recovery_deathrate_ds.cache()
recovery_deathrate_ds.show(100)
val w2 = Window.partitionBy($"Country").orderBy($"Daten".desc)
var recovery_deathrate_lastcases_ds = recovery_deathrate_ds.withColumn("rn", row_number.over(w2)).where($"rn" === 1).drop("rn") // take the first row of the window sorted by Date
val death_rate_ds = recovery_deathrate_lastcases_ds.select("Country", "Confirmed", "Deaths", "DeathRate").filter($"Confirmed" > 10000).sort($"DeathRate".desc) // filtering only the countries which has atleast 10000 confirmed cases to filter out very small countries with some infections
death_rate_ds.cache()
death_rate_ds.show(20)
val recovery_rate_ds = recovery_deathrate_lastcases_ds.select("Country", "Confirmed", "Recovered", "RecoveryRate").filter($"Confirmed" > 10000).sort($"RecoveryRate".desc) // filtering only the countries which has atleast 10000 confirmed cases to filter out very small countries with some infections
recovery_rate_ds.cache()
recovery_rate_ds.show(20)
var global_survival_death = recovery_deathrate_ds.groupBy("Daten").agg(sum($"Confirmed").as("Confirmed"), sum($"Recovered").as("Recovered"), sum($"Deaths").as("Deaths"))
global_survival_death = global_survival_death.withColumn("DeathRate", when($"Confirmed".equalTo(0), 0).otherwise($"Deaths"/$"Confirmed"))
global_survival_death = global_survival_death.withColumn("RecoveryRate", when($"Confirmed".equalTo(0), 0).otherwise($"Recovered"/$"Confirmed"))
global_survival_death.cache()
val duration_recovery_deathrate = (System.nanoTime - timestart_recoveryrate) / 1e9d
// duration in cluster: 6 seconds

// Vegas library is used locally to plot the dataframe. will not run in cluster since display is not available.
println("Time taken for recovery rate/death rate calculation:", duration_recovery_deathrate)
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

//////////////////////////////////////////////////// Analysis 4 ///////////////////////////////////////////////// 
// Distance based analysis - graphx, number of infections per 1000
// take last day's number of infections
// create a graph of countries
// when selecting a country - list nearby countries
// find the number of infections per 1000 of those nearby countries
val timestart_graph = System.nanoTime
val country = "Luxembourg"
val threshold_population = 500000
val w3 = Window.partitionBy($"Country").orderBy($"Daten".desc)
var lastcases_ds = cases_ds.withColumn("rn", row_number.over(w3)).where($"rn" === 1).drop("rn")
//lastcases_ds = lastcases_ds.filter($"Population" > threshold_population)

// create the country combinations
var country_combinations = lastcases_ds.select("Country", "Lat", "Long").withColumnRenamed("Country", "Country1").withColumnRenamed("Lat", "Lat1").withColumnRenamed("Long", "Long1").crossJoin(lastcases_ds.select("Country", "Lat", "Long")).withColumnRenamed("Country", "Country2").withColumnRenamed("Lat", "Lat2").withColumnRenamed("Long", "Long2")
country_combinations = country_combinations.filter($"Country1" =!= $"Country2")
def Distance = udf((lat1: Double, long1: Double, lat2: Double, long2: Double) => {
  val loc1 = Location(lat1, long1)
  val loc2 = Location(lat2, long2)
  new DistanceCalculatorImpl().calculateDistanceInKilometer(loc1, loc2)
} )
country_combinations = country_combinations.withColumn("Distance", Distance($"Lat1", $"Long1", $"Lat2", $"Long2"))
country_combinations = country_combinations.filter($"Country1" =!= $"Country2") // remove the combination is source country === destination country
country_combinations = country_combinations.filter($"Distance" < 700) // remove the combination is distance is greater than 700 - just a threshold
country_combinations = country_combinations.filter($"Distance" > 0)
country_combinations.select("Country1", "Country2", "Distance").coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(data_dir + "Country_Edge")
//country_combinations.cache()
country_combinations.filter($"Country1" === "Luxembourg").sort($"Distance".asc).show()
var country_combinations_new: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(data_dir + "Country_Edge")

// convert country combination to graphframe
var edge_ip = country_combinations_new.select("Country1", "Country2", "Distance").withColumnRenamed("Country1", "src").withColumnRenamed("Country2", "dst").withColumnRenamed("Distance", "relationship").toDF()
var country_distance_gf = GraphFrame.fromEdges(edge_ip)

// running shortest path algorithm in graphframe
var paths = country_distance_gf.shortestPaths.landmarks(Seq(country)).run()
paths = paths.select($"id", map_values($"distances")).filter(size($"map_values(distances)") > 0).select($"id", $"map_values(distances)".getItem(0))
paths.cache()
println("Directly impacting countries: ")

// interpretting the results from the shortest path algorithm
var shortest_path_countries = paths.filter($"map_values(distances)[0]" <= 2).sort($"map_values(distances)[0]".asc)
shortest_path_countries = shortest_path_countries.withColumnRenamed("id", "Country")
lastcases_ds = lastcases_ds.withColumn("NumInfectionsPer1000", $"Confirmed"*1000/$"Population")
shortest_path_countries = shortest_path_countries.join(lastcases_ds.select("Country", "NumInfectionsPer1000"), Seq("Country"), "left").sort($"NumInfectionsPer1000".desc)
shortest_path_countries.show(100)
val duration_graph = (System.nanoTime - timestart_graph) / 1e9d
// time taken in cluster: 47.8 seconds!

// page rank with graph x
// page rank algorithm outputs a probability distribution used to represent the likelihood that a person randomly traveling between countries will arrive in the targeted country.
// we expect the countries which is mostly connect
val timestart_pagerank = System.nanoTime
var country_distane_gx = country_distance_gf.toGraphX
val country_ranks = country_distane_gx.pageRank(0.0001, 0.15).vertices
val country_ranks_ranksort = country_ranks.sortBy(_._2, ascending=false)
val country_ranks_vertexsort = country_ranks.sortBy(_._1, ascending=true)
country_ranks_ranksort.take(10).foreach(tup => println(tup._1 + " has rank (pagerank): " + tup._2 + "."))
country_ranks_vertexsort.take(10).foreach(tup => println(tup._1 + " has rank (pagerank): " + tup._2 + "."))
val duration_pagerank = (System.nanoTime - timestart_pagerank) / 1e9d
// time taken in cluster = 264 seconds

// page rank from graphframe - reference: https://graphframes.github.io/graphframes/docs/_site/user-guide.html#label-propagation-algorithm-lpa
val timestart_pagerank_gf = System.nanoTime
val results = country_distance_gf.pageRank.resetProbability(0.15).tol(0.0001).run()
results.vertices.select("id", "pagerank").sort($"pagerank".desc).show()
results.edges.select("src", "dst", "weight").show()
val duration_pagerank_df = (System.nanoTime - timestart_pagerank_gf) / 1e9d
// time taken in cluster = 263 seconds = takes same time like with spark graphx
