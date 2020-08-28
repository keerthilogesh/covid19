import DFHelper.castColumnTo
import breeze.plot.{Figure, plot}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{FeatureHasher, RFormula, VectorAssembler}
import org.apache.spark.ml.stat._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType}
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.util.StatCounter
import vegas._
import scala.collection.JavaConverters._

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
val cc = "LU"
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
recovery_death_ds = recovery_death_ds.sort("Date")
recovery_death_ds.show(100)

import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

val plot1 = Vegas("Country Recovery Rate").
  withDataFrame(recovery_death_ds).
  encodeX("Date", Temp).
  encodeY("RecoveryRate", Quant).
  mark(Line).show

val plot2 = Vegas("Country Death Rate").
  withDataFrame(recovery_death_ds).
  encodeX("Date", Temp).
  encodeY("DeathRate", Quant).
  mark(Line).show

// predicting confirmed cases from mobility
val label_col = "Confirmed_change"
val feature_cols = Array("mobility", "mobility_week", "mobility_month", "mobility_change")
def plotDistributions(sampleSets: Seq[List[Double]], xlabel: String): Figure = {
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
var cases_mobility_ds_ml = castColumnTo(cases_mobility_ds, label_col, DoubleType)
cases_mobility_ds_ml = cases_mobility_ds_ml.withColumnRenamed(label_col, "label")
val plot_v = cases_mobility_ds_ml.select(collect_list(label_col)).first().getList[Double](0).asScala.toList
plotDistributions(Seq(plot_v), label_col)
val Array(trainData, valData, testData) = cases_mobility_ds_ml.randomSplit(Array(0.8, 0.1, 0.1), 123456)
val vector_assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")
val glr = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity").setMaxIter(10).setRegParam(0.3)
val pipeline = new Pipeline().setStages(Array(vector_assembler, glr))
val paramGrid = new ParamGridBuilder().build()
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)
val t1 = System.nanoTime
val cvModel = cv.fit(trainData)
val duration = (System.nanoTime - t1) / 1e9d
implicit class BestParamMapCrossValidatorModel(cvModel: CrossValidatorModel) {
  def bestEstimatorParamMap: ParamMap = {
    cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1
  }
}
println("============== Time ==============")
println(duration)
println("============== Best Parameters ==============")
println(cvModel.bestEstimatorParamMap)
val predictions = cvModel.transform(testData)
val evaluator_mae = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")
val mae = evaluator_mae.evaluate(predictions)
println("Test MAE = ", mae)
