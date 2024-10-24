package tyql.bench

import buildinfo.BuildInfo
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, query, Table as ScalaSQLTable}

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table, Query}
import tyql.Query.fix
import tyql.Expr.{IntLit, StringLit, min}
import Helpers.*

@experimental
class EvenOddQuery extends QueryBenchmark {
  override def name = "evenodd"
  override def set = true

  // TYQL data model
  type Number = (id: Int, value: Int)
  type NumberType = (value: Int, typ: String)
  type EvenOddDB = (numbers: Number)

  val tyqlDB = (
      numbers = Table[Number]("evenodd_numbers")
      )

  // Collections data model + initialization
  case class NumbersCC(id: Int, value: Int)
  case class ResultCC(value: Int, typ: String)
  def toCollRow(row: Seq[String]): NumbersCC = NumbersCC(row(0).toInt, row(1).toInt)
  case class CollectionsDB(numbers: Seq[NumbersCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.value.toString,
    r.typ.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "numbers" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("numbers"))

  //   ScalaSQL data model
  case class NumbersSS[T[_]](id: T[Int], value: T[Int])
  case class ResultSS[T[_]](value: T[Int], typ: T[String])
  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.value.toString,
    r.typ.toString
  )

  object evenodd_numbers extends ScalaSQLTable[NumbersSS]
  object evenodd_delta2 extends ScalaSQLTable[ResultSS]
  object evenodd_derived2 extends ScalaSQLTable[ResultSS]
  object evenodd_tmp2 extends ScalaSQLTable[ResultSS]
  object evenodd_delta1 extends ScalaSQLTable[ResultSS]
  object evenodd_derived1 extends ScalaSQLTable[ResultSS]
  object evenodd_tmp1 extends ScalaSQLTable[ResultSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val evenBase = tyqlDB.numbers.filter(n => n.value == 0).map(n => (value = n.value, typ = StringLit("even")).toRow)
    val oddBase = tyqlDB.numbers.filter(n => n.value == 1).map(n => (value = n.value, typ = StringLit("odd")).toRow)

    val (even, odd) = fix((evenBase, oddBase))((even, odd) =>
      val evenResult = tyqlDB.numbers.flatMap(num =>
        odd.filter(o => num.value == o.value + 1).map(o => (value = num.value, typ = StringLit("even")))
      ).distinct
      val oddResult = tyqlDB.numbers.flatMap(num =>
        even.filter(e => num.value == e.value + 1).map(e => (value = num.value, typ = StringLit("odd")))
      ).distinct
      (evenResult, oddResult)
    )

    val queryStr = odd.toQueryIR.toSQLString().replace("\"", "'")
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val evenBase = collectionsDB.numbers.filter(n => n.value == 0).map(n => ResultCC(value = n.value, typ = "even"))
    val oddBase = collectionsDB.numbers.filter(n => n.value == 1).map(n => ResultCC(value = n.value, typ = "odd"))

    val (even, odd) = FixedPointQuery.multiFix(set)((evenBase, oddBase), ((Seq(), Seq())))((even, odd) =>
      val evenResult = collectionsDB.numbers.flatMap(num =>
        odd.filter(o => num.value == o.value + 1).map(o => ResultCC(value = num.value, typ = "even"))
      ).distinct
      val oddResult = collectionsDB.numbers.flatMap(num =>
        even.filter(e => num.value == e.value + 1).map(e => ResultCC(value = num.value, typ ="odd"))
      ).distinct
      (evenResult, oddResult)
    )
    resultCollections = odd

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: ResultSS[?]) => (c.value, c.typ)

    val initBase = () =>
      val even = evenodd_numbers.select
        .filter(n => n.value === Expr(0))
        .map(n => (n.value, Expr("even")))
      val odd = evenodd_numbers.select
        .filter(n => n.value === Expr(1))
        .map(n => ( n.value, Expr("odd")))
      (even, odd)

    val fixFn: (ScalaSQLTable[ResultSS], ScalaSQLTable[ResultSS]) => (query.Select[(Expr[Int], Expr[String]), (Int, String)], query.Select[(Expr[Int], Expr[String]), (Int, String)]) =
      (even, odd) =>
        val evenResult = for {
          n <- evenodd_numbers.select
          o <- odd.join(n.value === _.value + 1)
        } yield (n.value, Expr("even"))

        val oddResult = for {
          n <- evenodd_numbers.select
          o <- even.join(n.value === _.value + 1)
        } yield (n.value, Expr("odd"))
        (evenResult, oddResult)

    FixedPointQuery.scalaSQLSemiNaive2(set)(
      db, (evenodd_delta1, evenodd_delta2), (evenodd_derived1, evenodd_derived2), (evenodd_tmp1, evenodd_tmp2)
    )(
      (toTuple, toTuple)
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[(ScalaSQLTable[ResultSS], ScalaSQLTable[ResultSS]) => (query.Select[Any, Any], query.Select[Any, Any])])

    val result = evenodd_derived2.select
    resultScalaSQL = db.run(result)


  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("value", "typ"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("value", "typ"), fromSSRes)

  // Extract all results to avoid lazy-loading
  //  def printResultJDBC(resultSet: ResultSet): Unit =
  //    println("Query Results:")
  //    while (resultSet.next()) {
  //      val x = resultSet.getInt("startNode")
  //      val y = resultSet.getInt("endNode")
  //      val z = resultSet.getArray("path")
  //      println(s"x: $x, y: $y, path=$z")
  //    }
}
