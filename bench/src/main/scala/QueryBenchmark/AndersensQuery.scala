package tyql.bench

import buildinfo.BuildInfo
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, Table as ScalaSQLTable, query}
import Helpers.*

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table}
import tyql.Expr.{IntLit, min}

import tyql.Dialect.ansi.given

@experimental
class AndersensQuery extends QueryBenchmark {
  override def name = "andersens"
  override def set = true
  if !set then ???

  // TYQL data model
  type Edge = (x: String, y: String)
  type AndersensDB = (addressOf: Edge, assign: Edge, loadT: Edge, store: Edge)

  val tyqlDB = (
    addressOf = Table[Edge]("andersens_addressOf"),
    assign = Table[Edge]("andersens_assign"),
    loadT = Table[Edge]("andersens_loadT"),
    store = Table[Edge]("andersens_store")
  )

  // Collections data model + initialization
  case class EdgeCC(x: String, y: String)
  def toCollRow(row: Seq[String]): EdgeCC = EdgeCC(row(0).toString, row(1).toString)
  case class CollectionsDB(addressOf: Seq[EdgeCC], assign: Seq[EdgeCC], loadT: Seq[EdgeCC], store: Seq[EdgeCC])
  def fromCollRes(r: EdgeCC): Seq[String] = Seq(
    r.x,
    r.y
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "addressOf" | "assign" | "loadT" | "store" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("addressOf"), tables("assign"), tables("loadT"), tables("store"))

  // ScalaSQL data model
  case class EdgeSS[T[_]](x: T[String], y: T[String])

  def fromSSRes(r: EdgeSS[?]): Seq[String] = Seq(
    r.x.toString,
    r.y.toString
  )

  object andersens_addressOf extends ScalaSQLTable[EdgeSS]
  object andersens_assign extends ScalaSQLTable[EdgeSS]
  object andersens_loadT extends ScalaSQLTable[EdgeSS]
  object andersens_store extends ScalaSQLTable[EdgeSS]
  object andersens_delta extends ScalaSQLTable[EdgeSS]
  object andersens_derived extends ScalaSQLTable[EdgeSS]
  object andersens_tmp extends ScalaSQLTable[EdgeSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[EdgeSS[?]] = null
  var resultCollections: Seq[EdgeCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.addressOf.map(a => (x = a.x, y = a.y).toRow)
    val query = base.unrestrictedFix(pointsTo =>
      tyqlDB.assign.flatMap(a =>
        pointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      )
        .union(tyqlDB.loadT.flatMap(l =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
              .map(pt2 =>
                (x = l.x, y = pt2.y).toRow
              )
          )
        ))
        .union(tyqlDB.store.flatMap(s =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
              .map(pt2 =>
                (x = pt1.y, y = pt2.y).toRow
              )
          )
        ))
    )
      .sort(_.y, Ord.ASC).sort(_.x, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.addressOf.map(a => EdgeCC(x = a.x, y = a.y))
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(pointsTo =>
      val v1 = collectionsDB.assign.flatMap(a =>
        pointsTo.filter(p => a.y == p.x).map(p =>
          EdgeCC(x = a.x, y = p.y)
        )
      )
      val v2 = collectionsDB.loadT.flatMap(l =>
        pointsTo.flatMap(pt1 =>
          pointsTo
            .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
            .map(pt2 =>
              EdgeCC(x = l.x, y = pt2.y)
            )
        )
      )
      val v3 = collectionsDB.store.flatMap(s =>
        pointsTo.flatMap(pt1 =>
          pointsTo
            .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
            .map(pt2 =>
              EdgeCC(x = pt1.y, y = pt2.y)
            )
        )
      )
      if set then (v1 ++ v2 ++ v3).distinct else v1 ++ v2 ++ v3
    )
      .sortBy(_.y).sortBy(_.x)

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    val initBase: () => query.Select[(Expr[String], Expr[String]), (String, String)] =
      () => andersens_addressOf.select.map(a => (a.x, a.y))

    val fixFn: ScalaSQLTable[EdgeSS] => query.Select[(Expr[String], Expr[String]), (String, String)] = (pointsTo) =>
      val innerQ1 = for {
        pointsTo <- pointsTo.select
        assign <- andersens_assign.join(_.y === pointsTo.x)
      } yield (assign.x, pointsTo.y)

      val innerQ2 = for {
        pointsTo1 <- pointsTo.select
        pointsTo2 <- pointsTo.join(pointsTo1.y === _.x)
        load <- andersens_loadT.join(_.y === pointsTo1.x)
      } yield (load.x, pointsTo2.y)

      val innerQ3 = for {
        pointsTo1 <- pointsTo.select
        pointsTo2 <- pointsTo.select.crossJoin()
        store <- andersens_store.join(s => s.x === pointsTo1.x && s.y === pointsTo2.x)
      } yield (pointsTo1.y, pointsTo2.y)

      innerQ1.union(innerQ2).union(innerQ3)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb,
      andersens_delta,
      andersens_tmp,
      andersens_derived
    )((c: EdgeSS[?]) => (c.x, c.y))(initBase.asInstanceOf[() => query.Select[Any, Any]])(
      fixFn.asInstanceOf[ScalaSQLTable[EdgeSS] => query.Select[Any, Any]]
    )

    val result = andersens_derived.select.sortBy(_.y).sortBy(_.x)
    resultScalaSQL = db.run(result)

    // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("x", "y"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("x", "y"), fromSSRes)

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
