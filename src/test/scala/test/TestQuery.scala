package test // test package so that it can be imported by bench. Use subpackages so that SBT test reporting is easier to read.
import tyql.{Aggregation, DatabaseAST, Query, ResultCategory, Table}

import language.experimental.namedTuples
import NamedTuple.AnyNamedTuple
import scala.util.Try
import scala.util.matching.Regex
import scala.util.boundary

class TestDatabase[Rows <: AnyNamedTuple] {
  def tables: NamedTuple.Map[Rows, Table] = ???
  def init(): String = ???
}

trait TestQuery[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): ReturnShape
  def expectedQueryPattern: String
}

object TestComparitor {

  def matchStrings(expectedQuery: String, actualQuery: String): (Boolean, String) = boundary {
    val placeholderPattern = "(\\w+)\\$(\\w+)".r

    // Step 1: Split the expected string by placeholders and get the list of placeholders
    val expectedParts = placeholderPattern.split(expectedQuery)
    val placeholders = placeholderPattern.findAllIn(expectedQuery).toList

    // Step 2: Initialize variables for building the transformed actual query
    var transformedActualQuery = actualQuery
    var currentPosition = 0
    val mappings = collection.mutable.Map.empty[String, collection.mutable.Map[String, String]]

    // Step 3: Process each placeholder
    for (placeholder <- placeholders) {
      val (variableName, placeholderKey) = {
        val parts = placeholder.split("\\$")
        (parts(0), parts(1))
      }
      val correspondingActualPattern = s"$variableName\\d+".r
      mappings.getOrElseUpdate(variableName, collection.mutable.Map.empty)

      // Find the matching variable in the actual query
      correspondingActualPattern.findFirstMatchIn(transformedActualQuery.substring(currentPosition)).foreach { matchActual =>
        val actualNumber = matchActual.matched.stripPrefix(variableName)
        val matchStart = matchActual.start + currentPosition
        val matchEnd = matchActual.end + currentPosition

        // Check if this placeholder has been seen before
        mappings(variableName).get(placeholder) match {
          case Some(mappedNumber) =>
            // If it has, ensure the same number is used
            if (mappedNumber != actualNumber) {
              val debugMessage = s"Expected $mappedNumber for $placeholder but found $actualNumber in actual query."
              boundary.break((false, debugMessage)) // Use boundary.break to return early
            }
          case None =>
            // If it's the first time seeing this placeholder, store the mapping
            if (mappings(variableName).values.exists(_ == actualNumber)) {
              val debugMessage = s"Multiple placeholders pointing to the same number: $actualNumber."
              boundary.break((false, debugMessage)) // Use boundary.break to return early
            }
            mappings(variableName).addOne(placeholder, actualNumber)
        }

        // Replace the actual variable (e.g., product373) with the placeholder (e.g., product$A)
        transformedActualQuery = transformedActualQuery.substring(0, matchStart) +
          placeholder +
          transformedActualQuery.substring(matchEnd)

        // Move currentPosition past this match
        currentPosition = matchEnd
      }
    }

    // Step 4: Compare the transformed actual query with the expected query
    if (transformedActualQuery == expectedQuery) {
      (true, "Queries match successfully.")
    } else {
      // Find the first mismatch index and extract context
      val mismatchIndex = transformedActualQuery.zip(expectedQuery).indexWhere { case (e, a) => e != a }
      val contextLength = 10
      val contextStart = math.max(0, mismatchIndex - contextLength)
      val contextEnd = math.min(transformedActualQuery.length, mismatchIndex + contextLength)

      val expectedContext = expectedQuery.slice(contextStart, contextEnd)
      val actualContext = transformedActualQuery.slice(contextStart, contextEnd)

      val debugMessage = s"Queries do not match at index $mismatchIndex.\n" +
        s"Expected: '...$expectedContext...'\n" +
        s"Actual  : '...$actualContext...'"

      (false, debugMessage)
    }
  }

}


trait TestSQLString[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]] extends munit.FunSuite with TestQuery[Rows, ReturnShape] {

  import tyql.TreePrettyPrinter.*
  test(testDescription) {
    val q = query()
    println(s"$testDescription:")
    val actualIR = q.toQueryIR
    val actual = actualIR.toSQLString().trim().replace("\n", " ").replaceAll("\\s+", " ")
    val strippedExpected = expectedQueryPattern.trim().replace("\n", " ").replaceAll("\\s+", " ")
    // Only print debugging trees if test fails
    val (success, debug) = TestComparitor.matchStrings(strippedExpected, actual)
//    println(actual) // print actual query even if passed, for manual testing
    if (!success)
      println(s"String match failed with: $debug")
      println(s"AST:\n${q.prettyPrint(0)}")
      println(s"IR: ${actualIR.prettyPrintIR(0, false)}") // set to true to print ASTs inline with IR
      println(s"\texpected: $strippedExpected") // because munit has annoying formatting
      println(s"\tactual  : $actual")

    assert(success, s"$debug")
  }
}

class TestSuiteTest extends munit.FunSuite {
  test("Compare strings without variables") {
    assert(TestComparitor.matchStrings("without variables 1 3 4", "without variables 1 3 4")._1)
  }
  test("Compare only variables no middle") {
    val s1 = "variable$A variable$B variable$C"
    val s2 = "variable1 variable2 variable3"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(r1._1, r1._2)
    val s3 = "variable$100 variable$B variable$200"
    val r2 = TestComparitor.matchStrings(s3, s2)
    assert(r2._1, r2._2)
  }
  test("Compare variables with intermediate text") {
    val s1 = "variable$A apple variable$B orange 1 3 4 variable$C"
    val s2 = "variable1 apple variable2 orange 1 3 4 variable3"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(r1._1, r1._2)
  }
  test("Compare variables with intermediate text with unchecked vars") {
    val s1 = "variable$A apple variable$B orange10 1 3 4 variable$C"
    val s2 = "variable1 apple variable2 orange10 1 3 4 variable3"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(r1._1, r1._2)
  }
  test("Compare different variable contexts") {
    val s1 = "var$A apple variable$B orange10 1 3 4 variable$C"
    val s2 = "var1 apple variable1 orange10 1 3 4 variable3"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(r1._1, r1._2)
  }
  test("Non-matching vars") {
    val s1 = "var$A apple var$A"
    val s2 = "var1 apple var2"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
  test("Repeated vars within one scope") {
    val s1 = "var$A apple var$B"
    val s2 = "var1 apple var1"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
  test("Repeated placeholder within different scope") {
    val s1 = "var$A apple var$B variable$B"
    val s2 = "var1 apple var2 variable3"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(r1._1, r1._2)
  }
  test("Difference between variables, expected") {
    val s1 = "var$A apple var$B var$B"
    val s2 = "var1 not apple var2 var2"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
  test("Difference between variables, expected 2") {
    val s1 = "var$A apple var$B var$B"
    val s2 = "var1 apple var2 a var2"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
  test("Difference between variables, actual") {
    val s1 = "var$A apple var$B a var$B"
    val s2 = "var1 apple var2 var2"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
  test("Different string no vars ") {
    val s1 = "string "
    val s2 = "int"
    val r1 = TestComparitor.matchStrings(s1, s2)
    assert(!r1._1, r1._2)
  }
}

abstract class SQLStringQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Query[Return, ?]] with TestQuery[Rows, Query[Return, ?]]
abstract class SQLStringAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Aggregation[?, Return]] with TestQuery[Rows, Aggregation[?, Return]]
