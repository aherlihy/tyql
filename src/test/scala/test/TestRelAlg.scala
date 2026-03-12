package test

import tyql.{DatabaseAST, Query, Aggregation}
import tyql.{RelAlgGenerator, TableSchema}

import language.experimental.namedTuples
import NamedTuple.AnyNamedTuple

/** Test trait for RelAlg MLIR generation.
  * Analogous to TestSQLString but generates relalg MLIR via scair.
  */
trait TestRelAlgString[Rows <: AnyNamedTuple, ReturnShape <: DatabaseAST[?]]
    extends munit.FunSuite
    with TestQuery[Rows, ReturnShape]:

  /** Table schemas for MLIR type annotations. */
  def tableSchemas: Map[String, TableSchema]

  /** Expected MLIR pattern (compared after whitespace normalization). */
  def expectedRelAlgPattern: String

  /** Optional: file name for writing MLIR output (e.g. "q1.mlir"). */
  def outputFileName: Option[String] = None

  test(testDescription + " [RelAlg]") {
    val q = query()
    val actualIR = q.toQueryIR
    val gen = RelAlgGenerator(tableSchemas)
    val actual = gen.convert(actualIR)
    val normalizedActual = actual.trim().replace("\n", " ").replaceAll("\\s+", " ")
    val normalizedExpected = expectedRelAlgPattern.trim().replace("\n", " ").replaceAll("\\s+", " ")

    println(s"$testDescription [RelAlg]:")
    println(actual)

    // Write MLIR to file if outputFileName is set
    outputFileName.foreach { name =>
      val outDir = java.nio.file.Paths.get("src/test/scala/test/relalg/out")
      java.nio.file.Files.createDirectories(outDir)
      val outPath = outDir.resolve(name)
      java.nio.file.Files.writeString(outPath, actual + "\n")
      println(s"Wrote MLIR to $outPath")
    }

    val (success, debug) = TestComparitor.matchStrings(normalizedExpected, normalizedActual)
    if (!success)
      println(s"RelAlg match failed: $debug")
      println(s"\texpected: $normalizedExpected")
      println(s"\tactual  : $normalizedActual")

    assert(success, s"$debug")
  }

abstract class RelAlgQueryTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows])
    extends TestRelAlgString[Rows, Query[Return]]
    with TestQuery[Rows, Query[Return]]:
  def expectedQueryPattern: String = "" // not used for RelAlg tests

abstract class RelAlgAggregationTest[Rows <: AnyNamedTuple, Return](using TestDatabase[Rows])
    extends TestRelAlgString[Rows, Aggregation[?, Return]]
    with TestQuery[Rows, Aggregation[?, Return]]:
  def expectedQueryPattern: String = "" // not used for RelAlg tests
