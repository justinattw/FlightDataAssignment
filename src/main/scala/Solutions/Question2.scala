/**
  * @author Justin Wong
  */

package Solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

/** Question 2: Find the names of the 100 most frequent flyers. */
object Question2 {

  /**
    * The function to solving Question 2.
    *
    * Solution 2: Using `flightDataDF`, count the number of records grouped by
    *             'passengerId' and ordered by 'count'.
    *             Join in `passengersDF` to get passenger names.
    *
    * @param flightDataDF : expects a joined flightDataDF-passengersDF dataframe with a 'passengerId' column.
    * @param topN  : calculates for the top N frequent flyers
    * @return : count of the total flights of top N passengers, and their names
    */
  def solve(flightDataDF: DataFrame, passengersDF: DataFrame, topN: Int): DataFrame = {

    // Get DF for the top N frequent flyers
    val topNFrequentFlyersDF: DataFrame = topNFrequentFlyers(flightDataDF, topN)
    joinInPassengerNames(topNFrequentFlyersDF, passengersDF)
  }

  /**
    * Calculates the number of records grouped by 'passengerID', then sorted desc
    *
    * @param DF   : expects a DF with a 'passengerId' column.
    * @param topN : top N frequent flyers to be calculated for
    * @return : DF with count of the total flights of top N passengers, and their associated names
    */
  def topNFrequentFlyers(DF: DataFrame, topN: Int): DataFrame = {
    DF.groupBy(col("passengerId"))
      .count()
      .orderBy(desc("count"))
      .limit(topN)
  }

  /**
    * Gets the passenger names by passengerId by joining in `passengersDF`
    *
    * @param DF           : `DF` with 'passengerId' column
    * @param passengersDF : `passengersDF` with 'passengerId' column and associated 'firstName' and 'lastName' columns
    * @return : the original DF but joined with passenger names based on 'passengerId'.
    */
  def joinInPassengerNames(DF: DataFrame, passengersDF: DataFrame): DataFrame = {
    DF
      .join(passengersDF, Seq("passengerId"), "left")  // left join just in case passengersDF is incomplete
      .select(
        col("passengerId").as("Passenger ID"),
        col("count").as("Number of Flights"),
        col("firstName").as("First name"),
        col("lastName").as("Last name"))
  }
}
