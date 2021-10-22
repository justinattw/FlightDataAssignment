/**
  * @author Justin Wong
  */

package Solutions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Question 4: Find the passengers who have been on more than 3 flights together. */
object Question4 {

  /**
    * The function to solving Question 4.
    *
    * Solution 4: Join `flightDataDF` with itself on flightId, matching passengers in a pairwise manner the same number
    *             of times that they share a flight together (by virtue of flightId).
    *
    *             Count number of records grouped by 'passenger1Id' and 'passenger2Id', aggregating the number of
    *             instances which the paired passengers share a flight.
    *
    *             Filter where count > N.
    *
    * @param DF                : expects `flightData` DF with 'passengerId' and 'flightId' columns.
    * @param atLeastNTimes     : the minimum number of flights two passengers shared
    * @return : count of the total flights of top N passengers, and their names
    */
  def solve(DF: DataFrame, atLeastNTimes: Int): DataFrame = {
    val pairwisePassengersDF = getPairwisePassengers(DF)
    val passengersFlightsSharedDF = getFlightsSharedByPairwisePassengers(pairwisePassengersDF)
    getMoreThanNFlightsShared(passengersFlightsSharedDF, atLeastNTimes)
  }

  /**
    * Joins the `flightDataDF` with itself on flightId, to match passengerIds in a pairwise manner. This actually
    * matches the passengerIds the same amount of times as they share a flight together (so not strictly pairwise).
    *
    * @param DF : a `flightData` DF
    * @return : a DF with the columns 'passenger1Id', 'passenger2Id', and 'date' (this is only used for the bonus question).
    */
  def getPairwisePassengers(DF: DataFrame): DataFrame = {
    // I've duplicated the DF just so I can rename the duplicate columns - otherwise Spark complains about 'ambiguous
    // reference'. TODO: probably better not to duplicate DF...
    val leftDF = DF.select("passengerId", "flightId")
      .withColumnRenamed("passengerId", "passenger1Id")
    val rightDF = DF.select("passengerId", "flightId", "date")
      .withColumnRenamed("passengerId", "passenger2Id")

    leftDF.join(rightDF, Seq("flightId"))
      .where(leftDF("passenger1Id") < rightDF("passenger2Id")) // no self-pairs, or duplicate p1-p2 pairs bidirectionally
      .drop(col("flightId"))
  }

  /**
    * Gets the number of flights shared by pairwise passengers, by grouping on passenger1Id and passenger2Id and count.
    *
    * @param DF : a pairwise passengers DF (where the number of records of each pairwise passenger is the same number
    *             of flights they have shared, achieved by a self-join of the `flightDataDF` on the 'flightId' column,
    *
    *             Example:
    *
    *                     passenger1Id | passenger2Id
    *                         12       |      13
    *                         12       |      13
    *                         23       |      25
    *                         23       |      25
    *                         18       |      21
    *                         18       |      23
    *
    * @return : a DF with all pairwise passengers' number of shared flights.
    *
    *           Example with above input DF:
    *
    *                     passenger1Id | passenger2Id | Number of flights together
    *                         12       |      13      |           2
    *                         23       |      25      |           2
    *                         18       |      21      |           1
    *                         18       |      23      |           1
    */
  def getFlightsSharedByPairwisePassengers(DF: DataFrame): DataFrame = {
    DF.groupBy(col("passenger1Id"), col("passenger2Id"))
      .count()
      .withColumnRenamed("count", "Number of flights together")
      .withColumnRenamed("passenger1Id", "Passenger 1 ID")
      .withColumnRenamed("passenger2Id", "Passenger 2 ID")
  }

  /**
    * Filters the DF to get only where number of flights shared > N.
    *
    * @param DF : a pairwise passenger DF with number of flights shared column
    * @param N  : the lowest number of flights shared you want
    * @return : a DF of pairwise passengers who have shared flights more than N times, and the actual number of times
    *           they have shared flights (+ any other additional columns)
    */
  def getMoreThanNFlightsShared(DF: DataFrame, N: Int): DataFrame = {
    DF.filter(col("Number of flights together") >= N)
  }
}
