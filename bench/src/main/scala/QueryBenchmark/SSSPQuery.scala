package tyql.bench

import buildinfo.BuildInfo
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, query, Table as ScalaSQLTable}
import Helpers.*

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table}
import tyql.Expr.min

@experimental
class SSSPQuery extends QueryBenchmark {
  override def name = "sssp"
  override def set = true
  if !set then ???

  // TYQL data model
  type WeightedEdge = (src: Int, dst: Int, cost: Int)
  type ResultEdge = (dst: Int, cost: Int)
  type WeightedGraphDB = (edge: WeightedEdge, base: ResultEdge)
  val tyqlDB = (
    edge = Table[WeightedEdge]("sssp_edge"),
    base = Table[ResultEdge]("sssp_base")
  )

  // Collections data model + initialization
  case class WEdgeCC(src: Int, dst: Int, cost: Int)
  case class ResultEdgeCC(dst: Int, cost: Int)
  def toCollRow(row: Seq[String]): WEdgeCC = WEdgeCC(row(0).toInt, row(1).toInt, row(2).toInt)
  def toCollRes(row: Seq[String]): ResultEdgeCC = ResultEdgeCC(row(0).toInt, row(1).toInt)
  case class CollectionsDB(base: Seq[ResultEdgeCC], edge: Seq[WEdgeCC])
  def fromCollRes(r: ResultEdgeCC): Seq[String] = Seq(
    r.dst.toString,
    r.cost.toString,
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "edge" =>
          loadCSV(csv, toCollRow)
        case "base" =>
          loadCSV(csv, toCollRes)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("base").asInstanceOf[Seq[ResultEdgeCC]], tables("edge").asInstanceOf[Seq[WEdgeCC]])

  //   ScalaSQL data model
  case class WEdgeSS[T[_]](src: T[Int], dst: T[Int], cost: T[Int])
  case class WResultEdgeSS[T[_]](dst: T[Int], cost: T[Int])

  def fromSSRes(r: WResultEdgeSS[?]): Seq[String] = Seq(
    r.dst.toString,
    r.cost.toString
  )

  object sssp_edge extends ScalaSQLTable[WEdgeSS]
  object sssp_base extends ScalaSQLTable[WEdgeSS]
  object sssp_delta extends ScalaSQLTable[WResultEdgeSS]
  object sssp_derived extends ScalaSQLTable[WResultEdgeSS]
  object sssp_tmp extends ScalaSQLTable[WResultEdgeSS]
  //

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[WResultEdgeSS[?]] = null
  var resultCollections: Seq[ResultEdgeCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.base
    val query = base.fix(sp =>
      tyqlDB.edge.flatMap(edge =>
        sp
          .filter(s => s.dst == edge.src)
          .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
      ).distinct
    )
      .aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow)
      .groupBySource(s => (dst = s._1.dst).toRow)
      .sort(_.cost, Ord.ASC)
      .sort(_.dst, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.base
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(sp =>
        collectionsDB.edge.flatMap(edge =>
          sp
            .filter(s => s.dst == edge.src)
            .map(s => ResultEdgeCC(dst = edge.dst, cost = s.cost + edge.cost))
        ).distinct
      )
      .groupBy(_.dst)
      .mapValues(_.minBy(_.cost))
      .values.toSeq
      .sortBy(_.cost)
      .sortBy(_.dst)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: WResultEdgeSS[?]) => (c.dst, c.cost)

    val initBase = () => sssp_base.select.map(e => (e.dst, e.cost))

    val fixFn: ScalaSQLTable[WResultEdgeSS] => query.Select[(Expr[Int], Expr[Int]), (Int, Int)] = path =>
      for {
        s <- path.select
        edge <- sssp_edge.join(s.dst === _.src)
      } yield (edge.dst, s.cost + edge.cost)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb, sssp_delta, sssp_tmp, sssp_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[WResultEdgeSS] => query.Select[Any, Any]])

    //    sssp_base.select.groupBy(_.dst)(_.dst) groupBy does not work with ScalaSQL + postgres
    backupResultScalaSql = ddb.runQuery(s"SELECT s.dst as dst, MIN(s.cost) as cost FROM ${ScalaSQLTable.name(sssp_derived)} as s GROUP BY s.dst ORDER BY dst, cost")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("dst", "cost"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("dst", "cost"), fromSSRes)

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
