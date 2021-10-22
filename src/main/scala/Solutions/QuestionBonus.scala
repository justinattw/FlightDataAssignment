/**
  * @author Justin Wong
  */

package Solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, max, min}

/** Bonus Question: Find the passengers who have been on more than N flights together within the range (from,to). */
object QuestionBonus {

  /**
    * The Main function (same functionality as 'solve')
    *
    * This bonus question is an extension of Question 4, as it asks for the dates which the paired passenger first
    * and last shared a flight. Therefore, some functions are shared.
    *
    * Solution 5: Join `flightDataDF` with itself on flightId, matching passengers in a pairwise manner the same number
    *             of times that they share a flight together (by virtue of flightId). Include the 'date' column in this
    *             join.
    *
    *             Grouping by 'passenger1Id' and 'passenger2Id', do count, max, and min all on the date column to
    *             aggregate the number of instances which the paired passengers share a flight, while also getting
    *             information on the earliest and latest flight date (as the DF is already within range).
    *
    *             Filter where count > N
    *
    *
    * @param DF            : expects `flightData` DF with 'passengerId' and 'flightId' columns.
    * @param atLeastNTimes : the minimum number of flights two passengers shared
    * @param from          : a string of the lower bound of date range, in the format "yyyy-MM-dd"
    * @param to            : a string of the upper bound of date range, in the format "yyyy-MM-dd"
    * @return : count of the total flights
    */
  def flownTogether(DF: DataFrame, atLeastNTimes: Int, from: String, to: String): DataFrame = {
    val inRangeDF = getInRangeDF(DF, from, to)
    val pairwisePassengersDF = Question4.getPairwisePassengers(inRangeDF)  // Uses Question 4 implementation
    val flightsSharedInRangeDF = getFlightsSharedByPairwisePassengersWithRange(pairwisePassengersDF)
    Question4.getMoreThanNFlightsShared(flightsSharedInRangeDF, atLeastNTimes)
  }

  /**
    * Filters the DF for records that occur after 'from' date and before 'to' date
    *
    * @param DF   : a Spark DataFrame with a 'date' column
    * @param from : the starting date of the date range where records are considered
    * @param to   : the ending date of the date range where records are considered
    * @return : a DF containing records where 'from <= date <= to'
    */
  def getInRangeDF(DF: DataFrame, from: String, to: String): DataFrame = {
    DF.filter(col("date") >= from && col("date") <= to)
  }

  /**
    * This is an extension of Question 4's 'getFlightsSharedByPairwisePassengers' function. Simply, on top of counting
    * the number of records grouped by passenger1Id and passenger2Id, it also finds the max and min of the date.
    *
    * @param DF : a pairwise passengers DF (where the number of records of each pairwise passenger is the same number
    *             of flights they have shared, achieved by a self-join of the `flightDataDF` on the 'flightId' column)
    *             and a date column.
    *
    *             Example:
    *
    *                     passenger1Id | passenger2Id |    date
    *                         12       |      13      | 2021-01-01
    *                         12       |      13      | 2021-01-05
    *                         23       |      25      | 2021-01-02
    *                         23       |      25      | 2021-01-09
    *                         18       |      21      | 2021-01-04
    *                         18       |      23      | 2021-01-24
    *
    *
    * @return : a DF with all pairwise passengers' number of shared flights, their earliest shared flight, and their
    *           latest shared flight.
    *
    *           Example with above input DF:
    *
    *                     passenger1Id | passenger2Id | Number of flights together |    From    |     To
    *                         12       |      13      |           2                | 2021-01-01 | 2021-01-05
    *                         23       |      25      |           2                | 2021-01-02 | 2021-01-09
    *                         18       |      21      |           1                | 2021-01-04 | 2021-01-04
    *                         18       |      23      |           1                | 2021-01-24 | 2021-01-24
    */
  def getFlightsSharedByPairwisePassengersWithRange(DF: DataFrame): DataFrame = {
    DF.groupBy(col("passenger1Id"), col("passenger2Id"))
      .agg(
        count("date").as("Number of flights together"),
        min("date").as("From"),
        max("date").as("To")
        )
      .withColumnRenamed("passenger1Id", "Passenger 1 ID")
      .withColumnRenamed("passenger2Id", "Passenger 2 ID")
  }

}
