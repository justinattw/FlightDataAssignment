package Solutions

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class Question3Test extends FunSuite {

  def testSolve(DF: DataFrame): Boolean = {
    true
  }

  /**
    * There is special consideration to ensure that this method, using collect_list() on Spark Windows, will preserve
    * ordering based on an alternate column 'date' ascending. Implementing this is important to the overall solution.
    *
    * @param DF :
    * @return
    */
  def testFlattenFromToColumnsIntoSequenceOfRowsList(DF: DataFrame): Boolean = {
    true
  }

  def testFindLongestRunSeqSeqUDF(): Boolean = {
    true
  }

  def findLongestRunFromFlightPathDFSeqSeqColumn(): Boolean = {
    true
  }

}
