package tyql.bench

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import language.experimental.namedTuples
import NamedTuple.*
import tyql.{Table, Ord}
import buildinfo.BuildInfo
import Helpers.*
import scala.jdk.CollectionConverters._

@experimental
class TCQuery extends QueryBenchmark {
  val name = "tc"
  val datadir = s"${BuildInfo.baseDirectory}/bench/data/$name"
  val outdir = s"$datadir/out"

  // TYQL data model
  type Edge = (x: Int, y: Int)
  type GraphDB = (edge: Edge)
  val tyqlDB = (
    edge = Table[Edge]("tc_edge")
  )

  // Collections data model
  case class EdgeCC(x: Int, y: Int)
  def toCollRow(row: Seq[String]): EdgeCC = EdgeCC(row(0).toInt, row(1).toInt)
  case class CollectionsDB(edge: Seq[EdgeCC])
  case class ResultEdge(startNode: Int, endNode: Int, path: Seq[Int])
  def fromCollRow(r: ResultEdge): Seq[String] = Seq(
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

  // ScalaSQL data model

  var resultTyql: ResultSet = null
  var resultScalaSQL: ResultSet = null
  var resultCollections: Seq[ResultEdge] = null

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
      .sort(p => p.path.length, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val path = collectionsDB.edge
      .filter(p => p.x == 1)
      .map(e => ResultEdge(e.x, e.y, Seq(e.x, e.y)))
    resultCollections = FixedPointQuery.fix(path, Seq())(path =>
      path.flatMap(p =>
        collectionsDB.edge
          .filter(e => p.endNode == e.x && !p.path.contains(e.y))
          .map(e => ResultEdge(startNode = p.startNode, endNode = e.y, p.path :+ e.y))
      ).distinct
    ).sortBy(r => r.path.length)

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    resultScalaSQL = ddb.runQuery("SELECT * FROM tc_edge")

  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("startNode", "endNode", "path"), fromCollRow)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    resultSetToCSV(resultScalaSQL, outfile)

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
