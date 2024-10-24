package tyql.bench

import buildinfo.BuildInfo
import scalasql.MySqlDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, query, Table as ScalaSQLTable}
import Helpers.*

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table}
import tyql.Expr.max

@experimental
class BOMQuery extends QueryBenchmark {
  override def name = "bom"
  override def set = false

  // TYQL data model
  type Assbl = (part: String, spart: String)
  type Basic = (part: String, days: Int)
  type BOMDB = (assbl: Assbl, basic: Basic)

  val tyqlDB = (
    assbl = Table[Assbl]("bom_assbl"),
    basic = Table[Basic]("bom_basic")
  )

  // Collections data model + initialization
  case class AssblCC(part: String, spart: String)
  case class BasicCC(part: String, days: Int)
  case class ResultCC(part: String, max: Int)
  def toCollRow1(row: Seq[String]): AssblCC = AssblCC(row(0).toString, row(1).toString)
  def toCollRow2(row: Seq[String]): BasicCC = BasicCC(row(0).toString, row(1).toInt)
  case class CollectionsDB(assbl: Seq[AssblCC], basic: Seq[BasicCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.part.toString,
    r.max.toString,
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "assbl" =>
          loadCSV(csv, toCollRow1)
        case "basic" =>
          loadCSV(csv, toCollRow2)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("assbl").asInstanceOf[Seq[AssblCC]], tables("basic").asInstanceOf[Seq[BasicCC]])

  //   ScalaSQL data model
  case class AssblSS[T[_]](part: T[String], spart: T[String])
  case class BasicSS[T[_]](part: T[String], days: T[Int])
  case class ResultSS[T[_]](part: T[String], max: T[Int])

  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.part.toString,
    r.max.toString
  )

  object bom_assbl extends ScalaSQLTable[AssblSS]
  object bom_basic extends ScalaSQLTable[BasicSS]
  object bom_delta extends ScalaSQLTable[ResultSS]
  object bom_derived extends ScalaSQLTable[ResultSS]
  object bom_tmp extends ScalaSQLTable[ResultSS]
  //

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val waitFor = tyqlDB.basic
    val query = waitFor.unrestrictedBagFix(waitFor =>
        tyqlDB.assbl.flatMap(assbl =>
          waitFor
            .filter(wf => assbl.spart == wf.part)
            .map(wf => (part = assbl.part, days = wf.days).toRow)))
      .aggregate(wf => (part = wf.part, max = max(wf.days)).toGroupingRow)
      .groupBySource(wf => (part = wf._1.part).toRow)
      .sort(_.part, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.basic.map(b => ResultCC(part = b.part, max = b.days))
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(waitFor =>
        collectionsDB.assbl.flatMap(assbl =>
          waitFor
            .filter(wf => assbl.spart == wf.part)
            .map(wf => ResultCC(part = assbl.part, max = wf.max))
        )
      )
      .groupBy(_.part)
      .mapValues(_.minBy(_.max))
      .values.toSeq
      .sortBy(_.part)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: ResultSS[?]) => (c.part, c.max)

    val initBase = () => bom_basic.select.map(e => (e.part, e.days))

    val fixFn: ScalaSQLTable[ResultSS] => query.Select[(Expr[String], Expr[Int]), (String, Int)] = waitFor =>
      for {
        wf <- waitFor.select
        assbl <- bom_assbl.join(_.spart === wf.part)
      } yield (assbl.part, wf.max)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      db, bom_delta, bom_derived, bom_tmp
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[ResultSS] => query.Select[Any, Any]])

    //    bom_base.select.groupBy(_.dst)(_.dst) groupBy does not work with ScalaSQL + postgres
    backupResultScalaSql = ddb.runQuery(s"SELECT s.part as part, MAX(s.max) as max FROM ${ScalaSQLTable.name(bom_derived)} as s GROUP BY s.part ORDER BY s.part")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("part", "max"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("part", "max"), fromSSRes)

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
