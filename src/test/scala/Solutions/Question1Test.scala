/**
  * This test file gives the skeleton of Scalatest and Junit.
  *
  */

package Solutions

import scala.language.implicitConversions
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

class Question1Test extends FunSuite {

  /**
    * Test that Solutions.Question1.solve() returns a correct DF with the variations of dummy data
    *
    * @param DF :
    * @return
    */
  def testSolve(DF: DataFrame): Boolean = {
    Question1.solve(DF)
    true
  }

  /**
    *
    *
    * @param DF :
    * @return
    */
  def testTotalFlightsByMonth(DF: DataFrame): Boolean = {
    true
  }
}

@RunWith(classOf[JUnitRunner])
class Question1UnitTests extends FunSuite {

  test("SampleTest1") {
    assert(1 == 1)
  }

}
