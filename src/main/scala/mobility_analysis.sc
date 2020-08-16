import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;
import spark.implicits._

// input files
val data_dir = "C:/Users/ac09983/Documents/Projects/covid/covid19/data/Big Data/big data project/"
val inputFile_mobility = data_dir + "Global_Mobility_Report.csv"
val inputFile_who = data_dir + "COVID-19-master/who_covid_19_situation_reports/who_covid_19_sit_rep_time_series/who_covid_19_sit_rep_time_series.csv"
val inputDir_daily_nonus = data_dir + "COVID-19-master/csse_covid_19_data/csse_covid_19_daily_reports"
val inputDir_daily_us = data_dir + "COVID-19-master/csse_covid_19_data/csse_covid_19_daily_reports_us"
val countrylookupFile = data_dir + "COVID-19-master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
val TimeSeriesGlobalConfirmed = data_dir + "COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
val TimeSeriesGlobalDeaths = data_dir + "COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
val TimeSeriesGlobalRecovered = data_dir + "COVID-19-master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"

// definitions
case class CountryLookup(val UID: scala.Option[scala.Int],
                         val iso2: scala.Option[scala.Predef.String],
                         val iso3: scala.Option[scala.Predef.String],
                         val code3: scala.Option[scala.Int],
                         val FIPS: scala.Option[scala.Int],
                         val Admin2: scala.Option[scala.Predef.String],
                         val Province_State: scala.Option[scala.Predef.String],
                         val Country_Region: scala.Option[scala.Predef.String],
                         val Lat: scala.Option[scala.Double],
                         val Long: scala.Option[scala.Double],
                         val Combined_Key: scala.Option[scala.Predef.String],
                         val Population: scala.Option[scala.Double]) {}

// read files
var country_lookup_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(countrylookupFile)
//val countryLookup = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(countrylookupFile).as(CountryLookup)
var mobility_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_mobility)
var who_df: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(inputFile_who)
var ts_global_confirmed: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(TimeSeriesGlobalConfirmed)
var ts_global_deaths: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(TimeSeriesGlobalDeaths)
var ts_global_recovered: DataFrame = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(TimeSeriesGlobalRecovered)

// rename column names

mobility_df = mobility_df.withColumnRenamed("country_region", "Country")
mobility_df = mobility_df.withColumnRenamed("country_region_code", "CC")
who_df = who_df.withColumnRenamed("Country/Region", "Country")
who_df = who_df.drop("WHO region", "Province/states")
country_lookup_df = country_lookup_df.withColumnRenamed("country_region", "Country")
country_lookup_df = country_lookup_df.withColumnRenamed("iso2", "CC")
ts_global_confirmed = ts_global_confirmed.withColumnRenamed("Country/Region", "Country")
ts_global_confirmed = ts_global_confirmed.drop("Province/State")
ts_global_deaths = ts_global_deaths.withColumnRenamed("Country/Region", "Country")
ts_global_deaths = ts_global_deaths.drop("Province/State")
ts_global_recovered = ts_global_recovered.withColumnRenamed("Country/Region", "Country")
ts_global_recovered = ts_global_recovered.drop("Province/State")

// ts data transformation - columns to rows and merging


// who data transformation - columns to rows
var yearcolumns = ArrayBuffer[String]();
var allcolumns: Array[String] = Array("Country");
var x = "";
for (x <- who_df.columns) {
  if (x.contains("20")) {
    yearcolumns += x;
  }
}
var yearcolumnsarray: Array[String] = yearcolumns.toArray
var selectcolumns: Array[String] = allcolumns ++ yearcolumns

def melt(df: DataFrame, id_vars: Seq[String], value_vars: Seq[String], var_name: String = "variable", value_name: String = "value"): DataFrame = {
  // Create array<struct<variable: str, value: ...>>
  val _vars_and_vals = array((for (c <- value_vars) yield {
    struct(lit(c).alias(var_name), col(c).alias(value_name))
  }): _*)
  // Add to the DataFrame and explode
  val _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
  val cols = id_vars.map(col) ++ {
    for (x <- List(var_name, value_name)) yield {
      col("_vars_and_vals")(x).alias(x)
    }
  }
  _tmp.select(cols: _*)
}

var who_df_new: DataFrame = melt(who_df, allcolumns, yearcolumns, "Date", "Cases")
who_df_new = who_df_new.na.drop(cols = Seq("Cases"))
who_df_new = who_df_new.groupBy("Country", "Date").agg(sum("Cases").as("Cases"))
who_df_new = who_df_new.withColumn("Daten", col("Date"))
for( a <- 1 to 9){
  who_df_new = who_df_new.withColumn("Daten", regexp_replace($"Daten", "^" + a.toString + "/", 0.toString + a.toString + "/"))
  who_df_new = who_df_new.withColumn("Daten", regexp_replace($"Daten", "/" + a.toString + "/", "/" + 0.toString + a.toString + "/"))
}
who_df_new = who_df_new.withColumn("Daten", to_date(col("Daten"), "MM/dd/yy"))
who_df_new = who_df_new.drop("Date")
var who_df_merged: DataFrame = who_df_new.join(country_lookup_df.select("CC", "Country").dropDuplicates(), Seq("Country"), "left")

// mobility transformation
mobility_df = mobility_df.withColumn("Date", to_date(col("date")))
mobility_df = mobility_df.withColumn("mobility", col("retail_and_recreation_percent_change_from_baseline") + col("grocery_and_pharmacy_percent_change_from_baseline") + col("parks_percent_change_from_baseline") + col("transit_stations_percent_change_from_baseline") + col("workplaces_percent_change_from_baseline") + col("residential_percent_change_from_baseline"))
mobility_df = mobility_df.groupBy("CC", "Country", "Date").mean("mobility")

// single country analysis
val cc = "DE"
mobility_df = mobility_df.filter($"CC" === cc)
who_df_merged = who_df_merged.filter($"CC" === cc)


