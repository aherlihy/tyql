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

trait TestSQLString[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]] extends munit.FunSuite with TestQuery[Rows, ReturnShape] {

  def matchStrings(expectedQuery: String, actualQuery: String): (Boolean, String) = {
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
    val (success, debug) = matchStrings(strippedExpected, actual)
    if (!success)
      println(s"String match failed with: $debug")
      println(s"AST:\n${q.prettyPrint(0)}")
      println(s"IR: ${actualIR.prettyPrintIR(0, false)}") // set to true to print ASTs inline with IR
      println(s"\texpected: $strippedExpected") // because munit has annoying formatting
      println(s"\tactual  : $actual")

    assert(success, s"$debug")
  }
}

abstract class SQLStringQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Query[Return]] with TestQuery[Rows, Query[Return]]
abstract class SQLStringAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Aggregation[Return]] with TestQuery[Rows, Aggregation[Return]]
