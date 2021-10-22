/**
  * @author Justin Wong
  *
  *         This project is for simple Exploratory data analysis into flight data. It is based on the Quantexa
  *         FlightDataAssignment.
  *
  *         The project employs Apache Spark to interpret discrete time-series data from a CSV into a Spark dataframe,
  *         and then draws exploratory insight from the data. There are 4 (+1 bonus) question to be extracted from the
  *         data and answered
  *
  *         Perusing this project can probably be accompanied by the README.md in the project root directory, and vice
  *         versa.
  */

import Solutions._

import scala.language.implicitConversions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    // SETUP

    // Suppress Info messages in Spark
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark: SparkSession = getCreateSparkSession

    // Load CSV data into Spark DFs
    val dataDir: String = "src/main/data/"

    val flightDataCSV: String = dataDir + "flightData.csv"
    val flightDataDF: DataFrame = loadCSV(spark, flightDataCSV)
//    flightDataDF.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))

    val passengersCSV: String = dataDir + "passengers.csv"
    val passengersDF: DataFrame = loadCSV(spark, passengersCSV)

    // -----------------------------------------------------------------------------------------------------------------

    // QUESTIONS AND SOLUTIONS

    // Question 1: Find the total number of flights for each month.
    val solution1DF: DataFrame = Question1.solve(flightDataDF)
    println("Solution 1")
    solution1DF.show()

    // Question 2: Find the names of the 100 most frequent flyers.
    val topN: Int = 100
    val solution2DF: DataFrame = Question2.solve(flightDataDF, passengersDF, topN)
    println("Solution 2")
    solution2DF.show()

    // Question 3: Find the greatest number of countries a passenger has been in without being in the UK (NotCountry).
    val excludeCountry: String = "uk"
    val solution3DF: DataFrame = Question3.solve(flightDataDF, excludeCountry)
    println("Solution 3")
    solution3DF.orderBy(desc("Longest run")).show()

    // Question 4: Find the passengers who have been on more than 3 flights together.
    val atLeastNTimesQ4: Int = 3
    val solution4DF:DataFrame = Question4.solve(flightDataDF, atLeastNTimesQ4)
    println("Solution 4")
    solution4DF.orderBy(desc("Number of flights together")).show()

    // Question 5 (Bonus)
    val atLeastNTimesQ5: Int = 3
    val fromDateString: String = "2017-01-01"  // format is yyyy-MM-dd
    val toDateString: String = "2017-03-01"
    val solution5BonusDF: DataFrame = QuestionBonus.flownTogether(flightDataDF, atLeastNTimesQ5, fromDateString, toDateString)
    println("Solution 5 (Bonus)")
    solution5BonusDF.orderBy(desc("Number of flights together")).show()


    // -----------------------------------------------------------------------------------------------------------------

    // (OVER)WRITE SOLUTIONS TO CSVs
    val outDir: String = "src/main/outSolutions/"
    writeCSV(solution1DF, outDir + "solution1")
    writeCSV(solution2DF, outDir + "solution2")
    writeCSV(solution3DF, outDir + "solution3")
    writeCSV(solution4DF, outDir + "solution4")
    writeCSV(solution5BonusDF, outDir + "solution5Bonus")

    // FINISH
    spark.stop()
  }


  // Helper functions for Main

  /**
    * Gets or creates a Spark session: "the entry point to programming Spark with the Dataset and DataFrame API".
    *
    * @return : a SparkSession object
    */
  def getCreateSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("FlightDataAssignment")
      .master("local[3]")
      .getOrCreate()
  }

  /**
    * Function to load CSV files into a Spark DF. The schema is automatically inferred by Spark.
    *
    * @param spark    : a Spark Session
    * @param dataFile : a CSV datafile to be converted into a Spark DF
    * @return : the CSV data read as a Spark DF
    */
  def loadCSV(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  /**
    * Write Spark DF as a CSV file.
    *
    * @param DF          : DF to be written as CSV
    * @param outFilePath : Filepath to save CSv
    */
  def writeCSV(DF: DataFrame, outFilePath: String): Unit = {
    DF.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(outFilePath)
  }
}