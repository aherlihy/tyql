package test // test package so that it can be imported by bench. Use subpackages so that SBT test reporting is easier to read.
import tyql.{DatabaseAST, Table, Query, Aggregation}

import language.experimental.namedTuples
import NamedTuple.AnyNamedTuple
import scala.util.Try
import scala.util.matching.Regex

class TestDatabase[Rows <: AnyNamedTuple] {
  def tables: NamedTuple.Map[Rows, Table] = ???
  def init(): Unit = ???
}

trait TestQuery[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]](using val testDB: TestDatabase[Rows]) {
  def testDescription: String
  def query(): ReturnShape
  def expectedQueryPattern: String
}

object TestComparitor {
  def matchStrings(expectedQuery: String, actualQuery: String): (Boolean, String) = {
    val placeholderPattern = "(\\w+)\\$(\\w+)".r

    // Step 1: Split expectedQuery into parts and extract placeholders
    val placeholders = placeholderPattern.findAllIn(expectedQuery).toList

    // Step 2: Initialize variables for building the transformed actual query
    var transformedActualQuery = actualQuery
    var currentPosition = 0

    // Step 3: Process each placeholder and replace the corresponding part in the actual query
    placeholders.foreach { placeholder =>
      val variableName = placeholder.split("\\$")(0)
      val correspondingActualPattern = s"$variableName\\d+".r

      // Find the matching variable in the actual query
      correspondingActualPattern.findFirstMatchIn(transformedActualQuery.substring(currentPosition)).foreach { m =>
        val matchStart = m.start + currentPosition
        val matchEnd = m.end + currentPosition

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

  def xmatchStrings(expectedQuery: String, actualQuery: String): (Boolean, String) = {
    val placeholderPattern = "(\\w+)\\$(\\w+)".r

    // Step 1: Split the expected string by placeholders and get the list of placeholders
    val expectedParts = placeholderPattern.split(expectedQuery)
    val placeholders = placeholderPattern.findAllIn(expectedQuery).toList

    // Build a version of the expected string without placeholders
    val cleanedExpectedQuery = expectedParts.mkString("")

    // Step 2: Initialize the cleaned version of the actual string and the map for placeholders
    var cleanedActualQuery = actualQuery
    var mappings = Map.empty[String, Map[String, String]]

    // Step 3: Process each placeholder
    for (placeholder <- placeholders) {
      val (variable, placeholderKey) = {
        val parts = placeholder.split("\\$")
        (parts(0), parts(1))
      }

      // Find the first occurrence of the variable with its associated number in the actual string
      val actualVariablePattern = s"\\b${variable}(\\d+)\\b".r
      actualVariablePattern.findFirstMatchIn(cleanedActualQuery) match {
        case Some(m) =>
          val actualNumber = m.group(1) // The number part (e.g., "27")

          // Ensure the variable has a mapping entry
          val currentMappings = mappings.getOrElse(variable, Map.empty)

          // Check if the placeholder already has a mapped number
          currentMappings.get(placeholderKey) match {
            case Some(existingNumber) =>
              if (existingNumber != actualNumber) {
                return (false, s"Mismatch for $variable$$$placeholderKey: expected $existingNumber, found $actualNumber.")
              }

            case None =>
              // No existing mapping, check if this number is already mapped to a different placeholder for this variable
              if (currentMappings.values.toSet.contains(actualNumber)) {
                return (false, s"Conflict for $variable: number $actualNumber is already mapped to a different placeholder.")
              }
              // Add the new mapping
              mappings = mappings.updated(variable, currentMappings.updated(placeholderKey, actualNumber))
          }

          // Remove the matched variable-number pair from the actual query
          cleanedActualQuery = cleanedActualQuery.replaceFirst(s"\\b${variable}${actualNumber}\\b", "")

        case None =>
          return (false, s"Could not find a match for $placeholder in the actual string.")
      }
    }

    // Step 4: Compare the cleaned versions of the expected and actual strings
    cleanedActualQuery = cleanedActualQuery.replaceAll("\\s+", " ").trim // Normalize whitespace
    val finalExpectedQuery = cleanedExpectedQuery.replaceAll("\\s+", " ").trim

    val splicedExpected = finalExpectedQuery.zipWithIndex.map {
      case (char, idx) => if (placeholders.exists(p => expectedQuery.indexOfSlice(p) == idx)) s"$$$char" else s"$char"
    }.mkString("")

    val splicedActual = cleanedActualQuery.zipWithIndex.map {
      case (char, idx) => if (placeholders.exists(p => expectedQuery.indexOfSlice(p) == idx)) s"$$$char" else s"$char"
    }.mkString("")

    // Check for mismatch
    if (splicedExpected == splicedActual) {
      (true, "Queries match successfully.")
    } else {
      // Find the first mismatch index and extract context
      val mismatchIndex = splicedExpected.zip(splicedActual).indexWhere { case (e, a) => e != a }
      val contextLength = 10
      val contextStart = math.max(0, mismatchIndex - contextLength)
      val contextEnd = math.min(splicedExpected.length, mismatchIndex + contextLength)

      val expectedContext = splicedExpected.slice(contextStart, contextEnd)
      val actualContext = splicedActual.slice(contextStart, contextEnd)

      val debugMessage = s"Queries do not match at index $mismatchIndex.\n" +
        s"Expected: '...$expectedContext...'\n" +
        s"Actual  : '...$actualContext...'"

      (false, debugMessage)
    }

  }


  import tyql.TreePrettyPrinter.*
  test(testDescription) {
    val q = query()
    println(s"$testDescription:")
    val actualIR = q.toQueryIR
    val actual = actualIR.toSQLString().trim().replace("\n", " ").replaceAll("\\s+", " ")
    val strippedExpected = expectedQueryPattern.trim().replace("\n", " ").replaceAll("\\s+", " ")
    // Only print debugging trees if test fails
    val (success, debug) = TestComparitor.matchStrings(strippedExpected, actual)
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
    assert(TestComparitor.matchStrings(s1, s2)._1)
    val s3 = "variable$100 variable$B variable$200"
    assert(TestComparitor.matchStrings(s3, s2)._1)
  }

  test("Compare variables with intermediate text") {
    val s1 = "variable$A apple variable$B orange 1 3 4 variable$C"
    val s2 = "variable1 apple variable2 orange 1 3 4 variable3"
    assert(TestComparitor.matchStrings(s1, s2)._1)
  }

  test("Compare variables with intermediate text with unchecked vars") {
    val s1 = "variable$A apple variable$B orange10 1 3 4 variable$C"
    val s2 = "variable1 apple variable2 orange10 1 3 4 variable3"
    assert(TestComparitor.matchStrings(s1, s2)._1)
  }

  test("Compare different variable contexts") {
    val s1 = "var$A apple variable$B orange10 1 3 4 variable$C"
    val s2 = "var1 apple variable1 orange10 1 3 4 variable3"
    assert(TestComparitor.matchStrings(s1, s2)._1)
  }

  test("Non-matching vars") {
    val s1 = "var$A apple var$A"
    val s2 = "var1 apple var2"
    assert(!TestComparitor.matchStrings(s1, s2)._1)
  }

  test("Repeated vars within one scope") {
    val s1 = "var$A apple var$B"
    val s2 = "var1 apple var1"
    assert(!TestComparitor.matchStrings(s1, s2)._1)
  }
}

abstract class SQLStringQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Query[Return]] with TestQuery[Rows, Query[Return]]
abstract class SQLStringAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Aggregation[Return]] with TestQuery[Rows, Aggregation[Return]]
