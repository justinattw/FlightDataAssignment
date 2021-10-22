/**
  * @author Justin Wong
  */

package Solutions

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{asc, col, collect_list, dense_rank, lit, max, struct, udf}

import scala.annotation.tailrec

/** Question 3: Find the greatest number of countries a passenger has been in without being in the UK.
  *             For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the
  *             correct answer would be 3 countries.
  */
object Question3 {

  /**
    * The function to solving Question 3.
    *
    * Assumptions to the data:
    * Assumption 1: Data includes passengers who only flies a maximum of a once a day. This is necessary as our datetime
    *               data does not contain information on the time, thus there is no definitive precise way for us to
    *               preserve the order of the data on the datetime level, only at the date level.
    *
    *               This assumption has been verified through data exploration in Excel pivot tables, but a test case
    *               to ensure assumption is followed in flightData should be implemented as a failsafe.
    *
    * Assumption 2: Data includes passengers who only travel between countries by flight. This means there can be no
    *               absence in passenger country-movement tracking data.
    *               For example: a traveller who flies UK -> CN, must fly CN -> US, then US -> FR. They may not cross
    *               the border by foot from CN to US, such that the data is represented as UK -> CN, US -> FR.
    *
    *               This assumption was verified only by eye test in a few cases of passengerIds. More robust testing
    *               will need to be implemented to affirm.
    *
    * Solution 3: Firstly, flatten the columns 'from' and 'to' into a tuple of tuples as 'flightPaths' in the form
    *             ((from, to), (from, to), ...), grouped by 'passengerId'.
    *             Then, for each row, apply a function to the 'flightPaths' column that traverses through the list of
    *             tuples while maintaining a count of consecutive elements without 'UK', and a maxCount of the highest
    *             count of consecutive elements.
    *
    * @param DF             : expects a `flightDataDF` dataframe with 'passengerId', 'from', 'to', and 'date' columns.
    * @param excludeCountry : separator country
    * @return : count of the total flights of top N passengers, and their names
    */
  def solve(DF: DataFrame, excludeCountry: String): DataFrame = {
    val passengerFlightListDF = flattenFromToColumnsIntoSequenceOfRowsList(DF)
    val solution3 = findLongestRunFromFlightPathDFSeqSeqColumn(passengerFlightListDF, excludeCountry)
    solution3
  }

  /**
    * Flattens two columns 'from' and 'to' into a Tuple of Tuples (technically a Sequence of Rows), creating an ordered
    * list of a passenger's flight history.
    *
    * Scala's F.collect_list() is non-deterministic, as "the order of collected results depends on the order of the rows
    * which may be non-deterministic after a shuffle.". Thus, collect_list() does not guarantee preservation of the
    * order of the collected list in a chronological sequence (as one would desire if iterating over an ordered DF), and
    * we have to carry out sort on the collected list ourselves.
    *
    * @param DF : a `passengerFlightDataDF` DF of the following form (neither passengerId nor date need to be ordered)
    *
    *                     passengerID  |  from  |   to   |    date
    *                         12       |   CG   |   IR   | 2017-01-01
    *                         12       |   IR   |   SE   | 2017-01-02
    *                         12       |   SE   |   PK   | 2017-01-03
    *                         14       |   SE   |   PK   | 2017-01-03
    *                         14       |   CN   |   SE   | 2017-01-01
    *                         18       |   NL   |   TJ   | 2017-01-05
    *
    * @return : a DF of the following form
    *
    *                     passengerID  |           flightPaths
    *                         12       |  ((CG, IR), (IR, SE), (SE, PK))
    *                         14       |      ((CN, SE), (SE, PK))  <-- note the date order of passenger 14's flights
    *                         18       |           ((NL, TJ))
    */
  def flattenFromToColumnsIntoSequenceOfRowsList(DF: DataFrame): DataFrame = {
    // Window is ordered by date
    val window = Window.partitionBy("passengerId").orderBy(asc("date"))

    // Perform collect_list() operation over the Window
    DF.withColumn("flightPath", collect_list(struct("from", "to")).over(window))
      .groupBy("passengerId")
      .agg(max(col("flightPath")) as "flightPath")
  }

  /**
    * This is a Spark UDF, extending the functions of the Spark framework. The function anticipates a Sequence of
    * Rows being the 'flightPath' that a passenger has traversed, and then finds the longest continuous sequence where
    * a given item country does not appear in the sequence.
    *
    * The time complexity of this function is O(N) on 'flightPath' sequence, as it traverses the sequence only once.
    *
    * Assumption: Assumption 2 (see solve() function) is currently made in this algorithm, as it only checks if the 'to'
    *             country != excludeCountry. The 'from' country is omitted from the evaluation, only because the
    *             assumption makes it unnecessary.
    *
    * @param flightPath     : expects a Seq[Row], which can be converted from the Spark ArrayType column type
    *                         example: ((CG, IR), (IR, SE), (SE, PK))
    *
    *                         ((FR, UK), (UK, ES), (ES, DE), (DE, UK))
    *                         ((UK, ES), (ES, DE), (DE, UK))
    *
    * @param excludeCountry : the country (code) that we wish to exclude from the passenger's runs
    * @return : an Integer of the longest run.
    */
  def findLongestRunOfFlightPathUDF: UserDefinedFunction = udf((flightPath: Seq[Row], excludeCountry: String) => {

    // Both tail-recursive and iterative implementations have been included, just uncomment out which one to use

    // TAIL-RECURSIVE IMPLEMENTATION

    // If the source (from) country of the first flight = excluded country, then currentRun starts at 0
    // flightPath.head(0) indexes on the first element ('from') of the first Row (from, to)
    val currentRun: Int = if (flightPath.head(0) == excludeCountry) 0 else 1
    tailRecursiveFindLongestRunOfFlightPath(flightPath, excludeCountry, currentRun, currentRun)

//    // ITERATIVE IMPLEMENTATION
//    iterativeFindLongestRunOfFlightPath(flightPath, excludeCountry)


  })

  /**
    * A tail-recursive implementation to find the longest run in a Sequence where a passenger has not visited a country
    * (i.e. where an element isn't seen).
    *
    * The flightPath list is truncated (removes the first element) at each recursive call to the function, and the
    * accumulator values currentRun and longestRun are maintained and evaluated at each call.
    *
    * The base case is triggered when flightPath is empty and returns the longestRun.
    *
    * Optimised with Scala tail-call optimisation (method annotation `@tailrec`).
    *
    * @param flightPath     : a Sequence of flights, which is represented as rows as (from, to) encoding information on
    *                         the departure and arrival countries.
    * @param excludeCountry : the country (code) to exclude from the consecutive run
    * @param currentRun     : the current sequential run (so far) without being in excludeCountry (accumulator value)
    * @param longestRun     : the longest sequential run (so far) without being in excludeCountry (accumulator value)
    * @return : integer of the longest sequential run overall without excludeCountry
    */
  @tailrec
  def tailRecursiveFindLongestRunOfFlightPath(flightPath: Seq[Row], excludeCountry: String, currentRun: Int, longestRun: Int): Int = {
    // Base case: if the Sequence evaluated is empty, then return the value of the longest run
    if (flightPath.isEmpty) {
      longestRun
    }
    else {
      // Need to maintain an evaluated current run val, because simply passing currentRun into the final recursion call
      // will not increment the accumulator value by 1.
      // Otherwise, two checks for `flightPath.head(1) == excludeCountry` will be needed for both currentRun and
      // longestRun arguments
      val evaluatedCurrentRun = if (flightPath.head(1) == excludeCountry) 0 else currentRun + 1

      // Recursive call
      tailRecursiveFindLongestRunOfFlightPath(
        flightPath.tail,  // flight path minus the head element (shortens sequence at each recursion level)
        excludeCountry,
        evaluatedCurrentRun,
        if (evaluatedCurrentRun + 1 > longestRun) evaluatedCurrentRun else longestRun)
    }
  }

  /**
    * An iterative implementation to find the longest run in a Sequence where a passenger has not visited a country
    * (i.e. where an element isn't seen).
    *
    * @param flightPath     : a Sequence of flights, which is represented as rows as (from, to) encoding information on
    *                         the departure and arrival countries.
    * @param excludeCountry : the country (code) to exclude from the consecutive run
    * @return : integer of the longest sequential run overall without excludeCountry
    */
  def iterativeFindLongestRunOfFlightPath(flightPath: Seq[Row], excludeCountry: String): Int = {
    // If the source (from) country of the first flight = excluded country, then currentRun starts at 0
    var currentRun: Int = if (flightPath.head(0) == excludeCountry) 0 else 1  // flightPath.head(0) indexes on the first element ('from') of the first tuple (from, to)
    var longestRun: Int = currentRun

    for (flight <- flightPath) {
      currentRun = if (flight(1) == excludeCountry) 0 else currentRun + 1
      longestRun = if (currentRun > longestRun) currentRun else longestRun
    }

    longestRun
  }

  /**
    * Apply the findLongestRunSeqSeqUDF user-defined function on the "flightPath" column, which is of type
    * ArrayType[StructType] (convertible to Seq[Row]).
    *
    * For each record, it finds the longest sequence where a passenger does not visit the excludedCountry.
    *
    * @param DF             :  expects a DF with a 'passengerId column, and 'flightPath' column, where flightPath is an
    *                          ArrayType[StructType] of the passengers historical flight sequence. Example:
    *
    *                          passengerID  |           flightPaths
    *                              12       |  ((CG, IR), (IR, SE), (SE, PK))
    *                              14       |      ((CN, SE), (SE, PK))
    *                              18       |           ((NL, TJ))
    *
    * @param excludeCountry : the String of the country that exclude from the passenger's runs
    * @return : DF with column 'passengerId'
    */
  def findLongestRunFromFlightPathDFSeqSeqColumn(DF: DataFrame, excludeCountry: String): DataFrame = {
    DF.withColumn("Longest Run", findLongestRunOfFlightPathUDF(col("flightPath"), lit(excludeCountry)))
      .select(col("passengerId").as("Passenger ID"), col("Longest run"))
  }

  // DEPRECATED - JUST TRYING SOMETHING OUT

  /**
    * This is just a rough implementation of an approach I thought of, to rank the ordered flight based on passengerId.
    * Having this information would allow subtraction of the ranks of excludeCountry to find the longest run a passenger
    * has been out of excludeCountry. However, this implementation  was not realised.
    *
    * @param DF : a `flightDataDF`
    * @return : a `flightDataDF` with a 'rank' column that numbers a passenger's flightId chronologically
    */
  def addRanksToCountryGroupByPassengerId(DF: DataFrame): DataFrame = {
    DF.withColumn("rank", dense_rank()
      .over(Window
        .partitionBy("passengerId")
        .orderBy(asc("date"))))
  }
}
