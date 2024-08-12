package test // test package so that it can be imported by bench. Use subpackages so that SBT test reporting is easier to read.
import tyql.{DatabaseAST, Table, Query, Aggregation}

import language.experimental.namedTuples
import NamedTuple.*
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
    var stringDebug = ""
    var placeholderMap = collection.mutable.Map.empty[Char, Int]
    val placeholderPattern: Regex = "\\$[A-Z]".r

    // Split stringA on placeholders and also extract the placeholders
    val parts = placeholderPattern.split(expectedQuery.trim())
    val placeholders = placeholderPattern.findAllIn(expectedQuery).toList
    println(placeholders)

    // Define initial position and result accumulator
    var currentPosition = 0
    var allMatches = true // Track if all matches are valid

    // Use a loop with a conditional check for continuation
    for ((part, i) <- parts.zipWithIndex if allMatches) {
      // Find the position of the current part in stringB
      val posInB = actualQuery.indexOf(part, currentPosition)
      if (posInB == -1) {
        stringDebug = s"'$part' not found within expected='${actualQuery.substring(currentPosition)}'"
        allMatches = false
      } else {
        // Update currentPosition to after the current part in stringB
        currentPosition = posInB + part.length

        if (i < placeholders.length) {
          val placeholder = placeholders(i).charAt(1) // Get 'A' from "$A"

          // Try to extract a number from the current position in stringB
          val numberPattern = "\\d+".r
          val numberMatch = numberPattern.findPrefixOf(actualQuery.substring(currentPosition))

          numberMatch match {
            case Some(numString) =>
              val num = Try(numString.toInt).getOrElse {
                stringDebug = s"Could not find $numberPattern"
                allMatches = false
                0 // Provide a default that indicates an issue, won't be used because of `allMatches`
              }
              if (allMatches) {
                currentPosition += numString.length
                placeholderMap.get(placeholder) match {
                  case Some(existingNum) =>
                    if (existingNum != num) {
                      stringDebug = s"Expected $existingNum but got $num for placeholder $placeholder"
                      allMatches = false
                    }
                  case None =>
                    println(s"placeholder=$placeholder, num=$num")
                    if (placeholderMap.values.exists(n => n == num)) {
                      stringDebug = s"Multiple placeholders pointing to the same number: $placeholder -> $num and ${placeholderMap.toSeq.filter((p, n) => n == num).map((p, n) => s"$p -> $n").mkString(", ")}"
                      allMatches = false
                    }
                    // Update the map with the new number for this placeholder
                    placeholderMap = placeholderMap.updated(placeholder, num)
                }
              }
            case None =>
              // Handle the case when no number is found, only if it's not the last part
              if (i != parts.length - 1) {
                stringDebug = s"Did not find $numberPattern"
                allMatches = false
              }
          }
        }
      }
    }

    // Check if the remainder of stringB matches the remainder of stringA
    if (allMatches && currentPosition != actualQuery.length && actualQuery.substring(currentPosition) != parts.last) {
      stringDebug = s"Leftover does not match, pos=$currentPosition, ${actualQuery.length}, restActual=${actualQuery.substring(currentPosition)}, restExpected=${parts.last}"
      allMatches = false
    }

    (allMatches, stringDebug)
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

    assert(success, s"expected '${strippedExpected}' but got '$actual'")
  }
}

abstract class SQLStringQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Query[Return]] with TestQuery[Rows, Query[Return]]
abstract class SQLStringAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows]) 
  extends TestSQLString[Rows, Aggregation[Return]] with TestQuery[Rows, Aggregation[Return]]
