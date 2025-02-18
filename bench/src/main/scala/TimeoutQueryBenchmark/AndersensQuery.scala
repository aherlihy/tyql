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

@experimental
class TOAndersensQuery extends QueryBenchmark {
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
    r.x.toString, r.y.toString
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
  var resultJDBC_RSQL: ResultSet = null
  var resultScalaSQL: Seq[EdgeSS[?]] = null
  var resultCollections: Seq[EdgeCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT andersens_addressOf1.x as x, andersens_addressOf1.y as y FROM andersens_addressOf as andersens_addressOf1) UNION ((SELECT andersens_assign3.x as x, ref2.y as y FROM andersens_assign as andersens_assign3, recursive1 as ref2 WHERE andersens_assign3.y = ref2.x) UNION (SELECT andersens_loadT6.x as x, ref6.y as y FROM andersens_loadT as andersens_loadT6, recursive1 as ref5, recursive1 as ref6 WHERE andersens_loadT6.y = ref5.x AND ref5.y = ref6.x) UNION (SELECT ref9.y as x, ref10.y as y FROM andersens_store as andersens_store11, recursive1 as ref9, recursive1 as ref10 WHERE andersens_store11.x = ref9.x AND andersens_store11.y = ref10.x)))\nSELECT * FROM recursive1 as recref0 ORDER BY x ASC, y ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.addressOf.map(a => (x = a.x, y = a.y).toRow)
    val query = base.unrestrictedFix(pointsTo =>
      tyqlDB.assign.flatMap(a =>
          pointsTo.filter(p => a.y == p.x).map(p =>
            (x = a.x, y = p.y).toRow))
        .union(tyqlDB.loadT.flatMap(l =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
              .map(pt2 =>
                (x = l.x, y = pt2.y).toRow))))
        .union(tyqlDB.store.flatMap(s =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
              .map(pt2 =>
                (x = pt1.y, y = pt2.y).toRow)))))
      .sort(_.y, Ord.ASC).sort(_.x, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0
    val base = collectionsDB.addressOf.map(a => EdgeCC(x = a.x, y = a.y))
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(pointsTo =>
      it += 1
      collectionsDB.assign.flatMap(a =>
          if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
          pointsTo.filter(p =>
            if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
            a.y == p.x).map(p =>
            if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
            EdgeCC(x = a.x, y = p.y)))
        .union(collectionsDB.loadT.flatMap(l =>
          if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
          pointsTo.flatMap(pt1 =>
            if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
            pointsTo
              .filter(pt2 =>
                if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
                l.y == pt1.x && pt1.y == pt2.x)
              .map(pt2 =>
                if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
                EdgeCC(x = l.x, y = pt2.y)))))
        .union(collectionsDB.store.flatMap(s =>
          if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
          pointsTo.flatMap(pt1 =>
            if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
            pointsTo
              .filter(pt2 =>
                if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
                s.x == pt1.x && s.y == pt2.x)
              .map(pt2 =>
                if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
                EdgeCC(x = pt1.y, y = pt2.y))))))
      .sortBy(_.y).sortBy(_.x)
      println(s"\nIT,$name,collections,$it")


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    val initBase: () => query.Select[(Expr[String], Expr[String]), (String, String)] =
      () => andersens_addressOf.select.map(a => (a.x, a.y))

    val fixFn: ScalaSQLTable[EdgeSS] => query.Select[(Expr[String], Expr[String]), (String, String)] = (pointsTo) =>
      it += 1
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
      ddb, andersens_delta, andersens_tmp, andersens_derived
    )((c: EdgeSS[?]) => (c.x, c.y))(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[EdgeSS] => query.Select[Any, Any]])

    val result = andersens_derived.select.sortBy(_.y).sortBy(_.x)
    resultScalaSQL = db.run(result)
    println(s"\nIT,$name,scalasql,$it")

  // Write results to csv for checking
  def writeJDBC_RSQLResult(): Unit =
    val outfile = s"$outdir/jdbc-rsql.csv"
    resultSetToCSV(resultJDBC_RSQL, outfile)

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
