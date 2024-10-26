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
class ASPSQuery extends QueryBenchmark {
  override def name = "asps"
  override def set = true

  // TYQL data model
  type WeightedEdge = (src: Int, dst: Int, cost: Int)
  type WeightedGraphDB = (edge: WeightedEdge)
  val tyqlDB = (
    edge = Table[WeightedEdge]("asps_edge"),
  )

  // Collections data model + initialization
  case class WEdgeCC(src: Int, dst: Int, cost: Int)
  def toCollRow(row: Seq[String]): WEdgeCC = WEdgeCC(row(0).toInt, row(1).toInt, row(2).toInt)
  case class CollectionsDB(edge: Seq[WEdgeCC])
  def fromCollRes(r: WEdgeCC): Seq[String] = Seq(
    r.src.toString,
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
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("edge"))

  //   ScalaSQL data model
  case class WEdgeSS[T[_]](src: T[Int], dst: T[Int], cost: T[Int])

  def fromSSRes(r: WEdgeSS[?]): Seq[String] = Seq(
    r.src.toString,
    r.dst.toString,
    r.cost.toString
  )

  object asps_edge extends ScalaSQLTable[WEdgeSS]
  object asps_base extends ScalaSQLTable[WEdgeSS]
  object asps_delta extends ScalaSQLTable[WEdgeSS]
  object asps_derived extends ScalaSQLTable[WEdgeSS]
  object asps_tmp extends ScalaSQLTable[WEdgeSS]
  //

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[WEdgeSS[?]] = null
  var resultCollections: Seq[WEdgeCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.edge
      .aggregate(e =>
        (src = e.src, dst = e.dst, cost = min(e.cost)).toGroupingRow)
      .groupBySource(e =>
        (src = e._1.src, dst = e._1.dst).toRow)

    val asps = base.unrestrictedFix(path =>
      path.aggregate(p =>
          path
            .filter(e =>
              p.dst == e.src)
            .aggregate(e =>
              (src = p.src, dst = e.dst, cost = min(p.cost + e.cost)).toGroupingRow))
        .groupBySource(p =>
          (g1 = p._1.src, g2 = p._2.dst).toRow).distinct
    )
    val query = asps
      .aggregate(a =>
        (src = a.src, dst = a.dst, cost = min(a.cost)).toGroupingRow)
      .groupBySource(p =>
        (g1 = p._1.src, g2 = p._1.dst).toRow)
      .sort(_.dst, Ord.ASC)
      .sort(_.src, Ord.ASC)
      .sort(_.cost, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)


  def executeCollections(): Unit =
    val base = collectionsDB.edge.groupBy(s => (s.src, s.dst)).mapValues(_.minBy(_.cost)).values.toSeq
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(path =>
      path.flatMap(p =>
        path
          .filter(e => p.dst == e.src)
          .map(e => WEdgeCC(p.src, e.dst, p.cost + e.cost))
          .groupBy(w => (w.src, w.dst))
          .mapValues(_.minBy(_.cost))
          .values.toSeq
      )
    )
      .groupBy(w => (w.src, w.dst))
      .mapValues(_.minBy(_.cost)).values.toSeq
      .sortBy(_.dst)
      .sortBy(_.src)
      .sortBy(_.cost)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: WEdgeSS[?]) => (c.src, c.dst, c.cost)

    val initBase = () =>
      //  workaround since groupBy does not work with ScalaSQL + postgres
      val initAgg = db.runRaw[(Int, Int, Int)](s"SELECT s.src, s.dst, MIN(s.cost) FROM ${ScalaSQLTable.name(asps_edge)} as s GROUP BY s.src, s.dst;")
      db.values(initAgg)

    val fixFn: ScalaSQLTable[WEdgeSS] => query.Select[(Expr[Int], Expr[Int], Expr[Int]), (Int, Int, Int)] = path =>
      val fixAgg = db.runRaw[(Int, Int, Int)](
        s"SELECT path1.src, path2.dst, MIN(path1.cost + path2.cost) FROM ${ScalaSQLTable.name(path)} path1, ${ScalaSQLTable.name(path)} path2 WHERE path1.dst = path2.src GROUP BY path1.src, path2.dst;"
      )
      if (fixAgg.isEmpty) // workaround scalasql doesn't allow empty values
         asps_tmp.select.map(c => (c.src, c.dst, c.cost))
      else
        db.values(fixAgg)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      db, asps_delta, asps_tmp, asps_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[WEdgeSS] => query.Select[Any, Any]])

    //  workaround since groupBy does not work with ScalaSQL + postgres
    backupResultScalaSql = ddb.runQuery(s"SELECT s.src as src, s.dst as dst, MIN(s.cost) as cost " +
      s"FROM ${ScalaSQLTable.name(asps_derived)} as s " +
      s"GROUP BY s.src, s.dst " +
      s"ORDER BY cost, src, dst;")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("src", "dst", "cost"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("src", "dst", "cost"), fromSSRes)

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
