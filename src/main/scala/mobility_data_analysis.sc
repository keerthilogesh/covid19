/*
Aim: The aim of this analysis is to analyze based on the merged cases and mobility dataset.
 */
import breeze.plot.{Figure, plot}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, LinearRegression}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.util.StatCounter
import vegas._
import vegas.sparkExt._
import vegas.render.WindowRenderer._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

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
val inputFile_mobility = data_dir + "Global_Mobility_Report.csv"
val inputFile_cases = data_dir + "WHO_Local_full/"

// read master data and mobility as dataframe and cast to dataset
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

//////////////////////////////////////////////////// Analysis 5 ///////////////////////////////////////////////// 
// merge mobility data and the cases dataset based on country and date
// add additional columns - weekly change, monthly change, daily change
// description of additional columns
// <column_name>_change = change in the value compared to previous day
// mobility_week = weekly average of the mobility feature
// mobility_month = monthly average of the mobility feature
// <other_columns>_week = weekly sum of the cases (for smoothing effect)
// <other_columns>_month = monthly sum of the cases (for smoothing effect)
val timestart_merge_mobility = System.nanoTime
var cases_mobility_ds = cases_ds.join(mobility_ds.select("CC", "Date", "mobility"), Seq("CC", "Date"), "left")
cases_mobility_ds = cases_mobility_ds.na.fill(0, Seq("mobility"))
cases_mobility_ds.cache()
val window_week =  Window.partitionBy("CC", "Country").orderBy(asc("Date")).rowsBetween(-6, 0) // weekly window - past six days and current day
val window_month =  Window.partitionBy("CC", "Country").orderBy(asc("Date")).rowsBetween(-30, 0) // monthly window - past 30 days and today
val window_diff = Window.partitionBy("CC", "Country").orderBy(asc("Date")) // current window
cases_mobility_ds = cases_mobility_ds.withColumn("Deaths_change", $"Deaths" - lag("Deaths", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("Recovered_change", $"Recovered" - lag("Recovered", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("Confirmed_change", $"Confirmed" - lag("Confirmed", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.withColumn("WHOCases_change", $"WHOCases" - lag("WHOCases", 1).over(window_diff))
cases_mobility_ds = cases_mobility_ds.select($"CC", $"Country", $"Date", $"mobility", $"Deaths", $"Confirmed", $"Recovered", $"WHOCases", $"WHOCases_change", $"Deaths_change", $"Confirmed_change", $"Recovered_change",
  avg($"mobility").over(window_week).as("mobility_week"),
  avg($"mobility").over(window_month).as("mobility_month"),
  sum($"Deaths_change").over(window_week).as("Deaths_week"),
  sum($"Deaths_change").over(window_month).as("Deaths_month"),
  sum($"Recovered_change").over(window_week).as("Recovered_week"),
  sum($"Recovered_change").over(window_month).as("Recovered_month"),
  sum($"WHOCases_change").over(window_week).as("WHOCases_week"),
  sum($"WHOCases_change").over(window_month).as("WHOCases_month"),
  sum($"Confirmed_change").over(window_week).as("Confirmed_month"),
  sum($"Confirmed_change").over(window_month).as("Confirmed_week"))
cases_mobility_ds = cases_mobility_ds.na.fill(0.0, Array("Confirmed", "Recovered", "WHOCases", "mobility", "Deaths",
  "Confirmed_week", "Recovered_week", "WHOCases_week", "mobility_week", "Deaths_week",
  "Confirmed_month", "Recovered_month", "WHOCases_month", "mobility_month", "Deaths_month",
  "Confirmed_change", "Recovered_change", "WHOCases_change", "Deaths_change"))
cases_mobility_ds.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(data_dir + "Cases_Mobility")
cases_mobility_ds.filter($"Country" === "Luxembourg").sort($"Date".desc).show()
var all_combined = cases_mobility_ds.select($"CC", $"Country", $"Date", $"mobility", $"Deaths", $"Confirmed", $"Recovered", $"WHOCases")
all_combined = all_combined.groupBy("Country").agg(max($"Confirmed").as("Confirmed"), max($"Deaths").as("Deaths"), max($"Recovered").as("Recovered"), max($"WHOCases").as("WHOCases"), avg($"mobility").as("mobility")).sort($"mobility".asc)
all_combined.cache()
all_combined.filter($"Confirmed" > 5000).sort($"mobility".asc).show()
val duration_merge_mobility = (System.nanoTime - timestart_merge_mobility) / 1e9d
println("Time taken for data merging:", duration_merge_mobility)
// Time taken in cluster : 30.5 seconds

// Vegas library is used locally to plot the dataframe. will not run in cluster since display is not available.
val lux_daily_case_change = cases_mobility_ds.filter($"Country" === "Luxembourg").filter($"Confirmed_change" >= 0).sort($"Date".desc)
val plot1: Unit = Vegas("Country Daily case change", width = 300.0, height=300.0).
  withDataFrame(lux_daily_case_change.select("Date", "Confirmed_change", "Deaths_change", "Recovered_change")).
  encodeX("Date", Temp).
  encodeY("Confirmed_change", Quant).
  mark(Line).show

//////////////////////////////////////////////////// Analysis 6 ///////////////////////////////////////////////// 
// Finding the correlation between the mobility and the cases 
// VectorAssembler is a transformer that combines a given list of columns into a single vector column.
val timestart_mobility_correlation = System.nanoTime
val lux_cases = cases_mobility_ds.filter($"Country" === "Luxembourg").filter($"Confirmed_change" >= 0).sort($"Date".desc)
val assembler_confirmed = new VectorAssembler().setInputCols(Array("Confirmed_month", "Confirmed_week", "Confirmed_change", "mobility", "mobility_week", "mobility_month")).setOutputCol("features")
val assembler_deaths = new VectorAssembler().setInputCols(Array("Deaths_month", "Deaths_week", "Deaths_change", "mobility", "mobility_week", "mobility_month")).setOutputCol("features")
val vector_confirmed = assembler_confirmed.transform(lux_cases)
val vector_deaths = assembler_deaths.transform(lux_cases)
// calculate correlation coefficient and print the full matrix
val Row(coeff11: Matrix) = Correlation.corr(vector_confirmed, "features").head
println("Pearson correlation matrix:\n" + coeff11.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff21: Matrix) = Correlation.corr(vector_confirmed, "features", "spearman").head
println("spearman correlation matrix:\n" + coeff21.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff12: Matrix) = Correlation.corr(vector_deaths, "features").head
println("Pearson correlation matrix:\n" + coeff12.toString(Int.MaxValue, Int.MaxValue))
val Row(coeff22: Matrix) = Correlation.corr(vector_deaths, "features", "spearman").head
println("spearman correlation matrix:\n" + coeff22.toString(Int.MaxValue, Int.MaxValue))
val duration_mobility_correlation = (System.nanoTime - timestart_mobility_correlation) / 1e9d
println("Time taken for mobility correlation:", duration_mobility_correlation)
// mobility has strong negative correlation with the new cases and the mortality rate
// people tend to move a little when the number of cases is decreasing
// Time taken in cluster : 13 seconds

//////////////////////////////////////////////////// Analysis 7 ///////////////////////////////////////////////// 
// predicting confirmed cases from mobility - since they are highly correlated with each other
def plotDistributions(sampleSets: Seq[List[Double]], xlabel: String): Figure = {
  /*
  This function is taken from the exercise to plot the distribution of the variables.
  */
  val f = Figure()
  val p = f.subplot(0)
  p.xlabel = xlabel
  p.ylabel = "Density"
  p.setXAxisDecimalTickUnits()
  p.setYAxisDecimalTickUnits()

  for (samples <- sampleSets) {
    val min = samples.min
    val max = samples.max
    val stddev = new StatCounter(samples).stdev
    val bandwidth = 1.06 * stddev * math.pow(samples.size, -.2)

    val domain = Range.Double(min, max, (max - min) / 100).toList.toArray
    val kd = new KernelDensity().
      setSample(spark.sparkContext.parallelize(samples)).
      setBandwidth(bandwidth)
    val densities = kd.estimate(domain)

    p += plot(domain, densities)
  }
  f
}
val label_col = "Confirmed_change"
val feature_cols = Array("mobility", "mobility_week", "mobility_month")
val seed = 123456
var cases_mobility_ds_ml = cases_mobility_ds.filter($"Country" === "Luxembourg").filter($"Confirmed_change" >= 0)
// var cases_mobility_ds_ml = cases_mobility_ds.filter($"Confirmed_change" >= 0) // accuracy is bad for all countries since country specific features like population is not added
cases_mobility_ds_ml = castColumnTo(cases_mobility_ds_ml, label_col, DoubleType)
cases_mobility_ds_ml = cases_mobility_ds_ml.withColumnRenamed(label_col, "label")
val plot_v = cases_mobility_ds_ml.select(collect_list("label")).first().getList[Double](0).asScala.toList
//plotDistributions(Seq(plot_v), label_col)
val Array(trainData, testData) = cases_mobility_ds_ml.randomSplit(Array(0.8, 0.2), seed=seed)

val vector_assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")

val glr = new GeneralizedLinearRegression()
val lr = new LinearRegression()

val pipeline_glr = new Pipeline().setStages(Array(vector_assembler, glr))
val pipeline_lr = new Pipeline().setStages(Array(vector_assembler, lr))

val paramGrid_glr = new ParamGridBuilder().addGrid(glr.family, Array("tweedie", "poisson")).addGrid(glr.link, Array("identity", "log")).addGrid(glr.maxIter, Array(10, 50, 100)).addGrid(glr.regParam, Array(0.1, 0.5, 0.7)).build()
val paramGrid_lr = new ParamGridBuilder().addGrid(lr.maxIter, Array(10, 50, 100)).addGrid(lr.regParam, Array(0.1, 0.5, 0.7)).build()

val cv_glr = new CrossValidator().setEstimator(pipeline_glr).setEvaluator(new RegressionEvaluator).setEstimatorParamMaps(paramGrid_glr).setNumFolds(3).setSeed(seed)
val cv_lr = new CrossValidator().setEstimator(pipeline_lr).setEvaluator(new RegressionEvaluator).setEstimatorParamMaps(paramGrid_lr).setNumFolds(3).setSeed(seed)

val t1 = System.nanoTime
val cvModel_glr = cv_glr.fit(trainData)
val cvModel_lr = cv_lr.fit(trainData)
val duration = (System.nanoTime - t1) / 1e9d

implicit class BestParamMapCrossValidatorModel(cvModel: CrossValidatorModel) {
  def bestEstimatorParamMap: ParamMap = {
    cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1
  }
}
println("============== Time for lr and glr ==============")
println(duration)
// duration for training all models with cross validation: 656 seconds for all countries but the accuracy is bad
// duration for training only Luxembourg data with cross validation : 279 seconds and the accuracy is good

val predictions_glr = cvModel_glr.transform(testData)
val predictions_lr = cvModel_lr.transform(testData)
val evaluator_mae = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")
val evaluator_r2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")

println("GLR, Test MAE = ", evaluator_mae.evaluate(predictions_glr))
println("LR, Test MAE = ", evaluator_mae.evaluate(predictions_lr))
println("GLR, Test R2 score = ", evaluator_r2.evaluate(predictions_glr))
println("LR, Test R2 score = ", evaluator_r2.evaluate(predictions_lr))

// Result: GeneralizedLinearRegression - MAE of test set = 26.90, R2 score of test set = 0.36
// Result: LinearRegression - MAE of test set = 32.12, R2 score of test set = 0.21
// As expected the GeneralizedLinearRegression has better performance than the Linear regression