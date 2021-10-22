package Data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.{Assertion, FunSuite}

class FlightDataTest extends FunSuite {

  // Setups will go here. In this case, we really just need the flightData.csv as a dataframe.

  /**
    * Test that each passenger only flies at most once a day
    *
    * Pseudocode: count groupBy "passengerId" and "date"
    *             select where count > 1
    *             assert resultant DF is empty
    *
    * @param DF : `flightDataDF`
    * @return : assertion that resultant DF is empty
    */
  def testOnlyOneFlightPerDayPerPassenger(DF: DataFrame): Assertion = {
    val testDF: DataFrame = DF.groupBy("passengerId", "date")
      .count()
      .where(col("count") > 1)

    assert(testDF.head(1).isEmpty)
  }

  /**
    * Test that a passenger only flies FROM the last country that it arrived in (TO).
    *
    * Pseudocode: collect_list on "from" column and "to" column separately (make sure to preserve order by date),
    *             groupBy 'passengerId'
    *             remove first element from 'fromList'
    *             remove last element from 'toList'
    *             assert 'fromList' == 'toList'
    *
    * @param DF : `flightDataDF`
    * @return : assertion that 'fromList' equals 'toList'
    */
  def testPassengerFromColumnEqualsLastTo(DF: DataFrame): Assertion = {
    // Pseudocode: collect_list on "from" column, and collect_list on "to" column separately (preserve date order), groupBy "passengerId"
    //             remove first element from 'fromList'
    //             remove final element from 'toList'
    //             assert 'fromList' == 'toList'

    // Or you can traverse every passengers' flightPath (Seq[Row]) and do, say, flightPath(i)(1) == flightPath(i+1)(0)

    assert(1 == 1)  // placeholder return
  }

  /**
    * Test that there are no null cells (can be extended to just checking certain columns have no nulls).
    *
    * @param DF : `flightDataDF`
    * @return : boolean of whether test passes or fails
    */
  def testNoNullValues(DF: DataFrame): Assertion = {
    assert(1 == 1)  // placeholder return
  }

}
