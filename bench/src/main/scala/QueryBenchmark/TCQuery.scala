package tyql.bench

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import language.experimental.namedTuples
import NamedTuple.*
import tyql.{Ord, Table}
import buildinfo.BuildInfo
import Helpers.*

import scala.jdk.CollectionConverters.*
import scalasql.{Table as ScalaSQLTable, Expr, query}
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax

@experimental
class TCQuery extends QueryBenchmark {
  override def name = "tc"

  // TYQL data model
  type Edge = (x: Int, y: Int)
  type GraphDB = (edge: Edge)
  val tyqlDB = (
    edge = Table[Edge]("tc_edge")
  )

  // ScalaSQL data model
  case class EdgeSS[T[_]](x: T[Int], y: T[Int])
  case class ResultEdgeSS[T[_]](startNode: T[Int], endNode: T[Int], path: T[String])
  def fromSSRow(r: ResultEdgeSS[?]): Seq[String] = Seq(
    r.startNode.toString,
    r.endNode.toString,
    r.path.toString
  )
  object tc_edge extends ScalaSQLTable[EdgeSS]
  object tc_delta extends ScalaSQLTable[ResultEdgeSS]
  object tc_derived extends ScalaSQLTable[ResultEdgeSS]
  object tc_tmp extends ScalaSQLTable[ResultEdgeSS]

  // Collections data model + initialization
  case class EdgeCC(x: Int, y: Int)
  def toCollRow(row: Seq[String]): EdgeCC = EdgeCC(row(0).toInt, row(1).toInt)
  case class CollectionsDB(edge: Seq[EdgeCC])
  case class ResultEdgeCC(startNode: Int, endNode: Int, path: Seq[Int])
  def fromCollRow(r: ResultEdgeCC): Seq[String] = Seq(
    r.startNode.toString,
    r.endNode.toString,
    r.path.mkString("[", ", ", "]")
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).sortBy(_._1).map((name, csv) =>
      name match
        case "edge" =>
          loadCSV(csv, toCollRow)
        case _ => ???
    )
    collectionsDB = CollectionsDB(tables(0))

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultEdgeSS[?]] = null
  var resultCollections: Seq[ResultEdgeCC] = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val pathBase = tyqlDB.edge
      .filter(p => p.x == 1)
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)

    val query = pathBase.unrestrictedBagFix(path =>
        path.flatMap(p =>
          tyqlDB.edge
            .filter(e => e.x == p.endNode && !p.path.contains(e.y))
            .map(e =>
              (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow)))
      .sort(p => p.path.length, Ord.ASC).sort(p => p.startNode, Ord.ASC).sort(_.endNode, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val path = collectionsDB.edge
      .filter(p => p.x == 1)
      .map(e => ResultEdgeCC(e.x, e.y, Seq(e.x, e.y)))
    resultCollections = FixedPointQuery.fix(path, Seq())(path =>
      path.flatMap(p =>
        collectionsDB.edge
          .filter(e => p.endNode == e.x && !p.path.contains(e.y))
          .map(e => ResultEdgeCC(startNode = p.startNode, endNode = e.y, p.path :+ e.y))
      ).distinct
    ).sortBy(r => r.path.length).sortBy(_.startNode).sortBy(_.endNode)

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    def initList(v1: Expr[Int], v2: Expr[Int]): Expr[String] = Expr { implicit ctx => sql"[$v1, $v2]" }
    def listAppend(v: Expr[Int], lst: Expr[String]): Expr[String] = Expr { implicit ctx => sql"list_append($lst, $v)" }
    def listContains(v: Expr[Int], lst: Expr[String]): Expr[Boolean] = Expr { implicit ctx => sql"list_contains($lst, $v)" }
    def listLength(lst: Expr[String]): Expr[Int] = Expr { implicit ctx => sql"length($lst)" }
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: ResultEdgeSS[?]) => (c.startNode, c.endNode, c.path)

    val initBase = () =>
      tc_edge.select
        .filter(_.x === Expr(1))
        .map(e => (e.x, e.y, initList(e.x, e.y)))

    val fixFn: ScalaSQLTable[ResultEdgeSS] => query.Select[(Expr[Int], Expr[Int], Expr[String]), (Int, Int, String)] = path =>
      for {
        p <- path.select
        e <- tc_edge.join(p.endNode === _.x)
        if !listContains(e.y, p.path)
      } yield (p.startNode, e.y, listAppend(e.y, p.path))

    FixedPointQuery.scalaSQLSemiNaive(
      db, tc_delta, tc_derived, tc_tmp
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[ResultEdgeSS] => query.Select[Any, Any]])


    val result = tc_derived.select.sortBy(_.path).sortBy(_.endNode).sortBy(_.startNode)
    resultScalaSQL = db.run(result)

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("startNode", "endNode", "path"), fromCollRow)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    collectionToCSV(resultScalaSQL, outfile, Seq("startNode", "endNode", "path"), fromSSRow)

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
