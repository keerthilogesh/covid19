import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

val spark: SparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate

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

def MeltDF(ip_df: DataFrame, op_col: String, melt_col: String, melt_col_string: String): DataFrame = {
  var year_columns = ArrayBuffer[String]()
  for (x <- ip_df.columns) {
    if (x.contains(melt_col_string)) {
      year_columns += x
    }
  }
  var all_columns: Array[String] = ip_df.columns.toSet.diff(year_columns.toSet).toArray
  print(all_columns)

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

  var df_new: DataFrame = melt(ip_df, all_columns, year_columns, melt_col, op_col)
  df_new = df_new.na.drop(cols = Seq(op_col))
  val groupby_columns = all_columns :+ melt_col
  df_new = df_new.groupBy(groupby_columns.head, groupby_columns.tail: _*).agg(sum(op_col).as(op_col))
  df_new
}

def ChangeDateFormat(ip_df: DataFrame, date_col: String): DataFrame = {
  var ip_dfn = ip_df.withColumn("Daten", col(date_col))
  for (a <- 1 to 9) {
    ip_dfn = ip_dfn.withColumn("Daten", regexp_replace($"Daten", "^" + a.toString + "/", 0.toString + a.toString + "/"))
    ip_dfn = ip_dfn.withColumn("Daten", regexp_replace($"Daten", "/" + a.toString + "/", "/" + 0.toString + a.toString + "/"))
  }
  ip_dfn = ip_dfn.withColumn("Daten", to_date(col("Daten"), "MM/dd/yy"))
  ip_dfn = ip_dfn.drop(date_col)
  ip_dfn
}


var ts_global_confirmed_df = MeltDF(ts_global_confirmed, "Confirmed", "Date", "20")
ts_global_confirmed_df = ChangeDateFormat(ts_global_confirmed_df, "Date")
var ts_global_deaths_df = MeltDF(ts_global_deaths, "Deaths", "Date", "20")
ts_global_deaths_df = ChangeDateFormat(ts_global_deaths_df, "Date")
var ts_global_recovered_df = MeltDF(ts_global_recovered, "Recovered", "Date", "20")
ts_global_recovered_df = ChangeDateFormat(ts_global_recovered_df, "Date")
var who_df_new = MeltDF(who_df, "WHOCases", "Date", "/20")
who_df_new = ChangeDateFormat(who_df_new, "Date")

// ts global merged
var ts_global = ts_global_confirmed_df.join(ts_global_deaths_df, Seq("Country", "Lat", "Long", "Daten"), "outer").join(ts_global_recovered_df, Seq("Country", "Lat", "Long", "Daten"), "outer")
ts_global = ts_global.sort("Country", "Daten")

country_lookup_df = country_lookup_df.groupBy("CC", "Country").agg(sum($"Population").as("Population"), avg($"Lat").as("Lat"), avg($"Long_").as("Long"))
var who_df_merged: DataFrame = who_df_new.join(country_lookup_df.select("CC", "Country").dropDuplicates(), Seq("Country"), "left")
ts_global = ts_global.join(country_lookup_df.select("CC", "Country", "Population").dropDuplicates(), Seq("Country"), "left")

// who and ts merged
var full_ds = ts_global.join(who_df_merged.select("CC", "Daten", "WHOCases").dropDuplicates(), Seq("CC", "Daten"), "left")
full_ds.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(data_dir + "WHO_Local_full")
