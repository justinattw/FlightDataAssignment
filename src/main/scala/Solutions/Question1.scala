/**
  * @author Justin Wong
  */


package Solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, month}

/** Question 1: Find the total number of flights for each month. */
object Question1 {

  /**
    * The function to solving Question 2.
    *
    * Solution 1: Drop duplicate columns of 'flightId'.
    *             Then, count the number of records DF grouped by month (via the date column).
    *
    * @param DF : expecting flightDataDF with a 'date' column
    * @return : Dataframe of month and its associated count of the number of flights in each month.
    */
  def solve(DF: DataFrame): DataFrame = {
    val uniqueFlightsDF: DataFrame = getUniqueFlights(DF)
    findTotalFlightsByMonth(uniqueFlightsDF)
  }

  def getUniqueFlights(DF: DataFrame): DataFrame = {
    DF.dropDuplicates("flightId")
  }

  /**
    * Counts number of records (flights) grouped by month, then sorted asc by month
    *
    * @param DF : expects a DF with a 'date' column
    * @return : DF of month and its associated count of records (becoming number of flights) in each month.
    */
  def findTotalFlightsByMonth(DF: DataFrame): DataFrame = {
    DF.withColumn("Month", month(col("date")))  // extract month from date col into new 'Month' col
      .groupBy(col("Month"))
      .count()
      .orderBy(col("Month").asc)
      .select(col("Month"), col("count").as("Number of Flights"))
  }
}
