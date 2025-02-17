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
class TOSSSPQuery extends QueryBenchmark {
  override def name = "sssp"
  private val outputHeader = Seq("dst", "cost")
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
  var resultJDBC_RSQL: ResultSet = null
  var resultJDBC_SNE: ResultSet = null
  var resultScalaSQL: Seq[WResultEdgeSS[?]] = null
  var resultCollections: Seq[ResultEdgeCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT * FROM sssp_base as sssp_base1) UNION ((SELECT sssp_edge3.dst as dst, ref1.cost + sssp_edge3.cost as cost FROM sssp_edge as sssp_edge3, recursive1 as ref1 WHERE ref1.dst = sssp_edge3.src)))\n SELECT recref0.dst as dst, MIN(recref0.cost) as cost FROM recursive1 as recref0 GROUP BY recref0.dst ORDER BY dst ASC, cost ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

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
    var it = 0
    val base = collectionsDB.base
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(sp =>
        it += 1
        collectionsDB.edge.flatMap(edge =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          sp
            .filter(s =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              s.dst == edge.src)
            .map(s =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              ResultEdgeCC(dst = edge.dst, cost = s.cost + edge.cost))
        ).distinct
      )
      .groupBy(_.dst)
      .mapValues(_.minBy(_.cost))
      .values.toSeq
      .sortBy(_.cost)
      .sortBy(_.dst)
    println(s"\nIT,$name,collections,$it")


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: WResultEdgeSS[?]) => (c.dst, c.cost)

    val initBase = () => sssp_base.select.map(e => (e.dst, e.cost))

    val fixFn: ScalaSQLTable[WResultEdgeSS] => query.Select[(Expr[Int], Expr[Int]), (Int, Int)] = path =>
      it += 1
      for {
        s <- path.select
        edge <- sssp_edge.join(s.dst === _.src)
      } yield (edge.dst, s.cost + edge.cost)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb, sssp_delta, sssp_tmp, sssp_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[WResultEdgeSS] => query.Select[Any, Any]])

    //    sssp_base.select.groupBy(_.dst)(_.dst) groupBy does not work with ScalaSQL + postgres
    backupResultScalaSql = ddb.runQuery(s"SELECT s.dst as dst, MIN(s.cost) as cost FROM ${ScalaSQLTable.name(sssp_derived)} as s GROUP BY s.dst ORDER BY dst, cost")
    println(s"\nIT,$name,scalasql,$it")

  // Write results to csv for checking
  def writeBenchResult(mode: QueryMode): Unit =
    mode match
      case QueryMode.TyQL =>
        val outfile = s"$outdir/tyql.csv"
        resultSetToCSV(resultTyql, outfile)
      case QueryMode.ScalaSQL =>
        val outfile = s"$outdir/scalasql.csv"
        if (backupResultScalaSql != null)
          resultSetToCSV(backupResultScalaSql, outfile)
        else
          collectionToCSV(resultScalaSQL, outfile, outputHeader, fromSSRes)
      case QueryMode.Collections =>
        val outfile = s"$outdir/collections.csv"
        collectionToCSV(resultCollections, outfile, outputHeader, fromCollRes)
      case QueryMode.JDBC_SNE =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_SNE, outfile)
      case QueryMode.JDBC_RSQL =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_RSQL, outfile)

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
