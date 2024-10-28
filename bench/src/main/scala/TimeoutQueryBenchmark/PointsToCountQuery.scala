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
import tyql.{Ord, Query, Table}
import tyql.Query.{fix, unrestrictedFix}
import tyql.Expr.{IntLit, StringLit, min, sum}
import Helpers.*

@experimental
class TOPointsToCountQuery extends QueryBenchmark {
  override def name = "pointstocount"
  override def set = true
  if !set then ???

  // TYQL data model
  type ProgramHeapOp = (x: String, y: String, h: String)
  type ProgramOp = (x: String, y: String)
  type PointsToDB = (newT: ProgramOp, assign: ProgramOp, loadT: ProgramHeapOp, store: ProgramHeapOp, baseHPT: ProgramHeapOp)
  val tyqlDB = (
    newT = Table[ProgramOp]("pointstocount_new"),
    assign = Table[ProgramOp]("pointstocount_assign"),
    loadT = Table[ProgramHeapOp]("pointstocount_loadT"),
    store = Table[ProgramHeapOp]("pointstocount_store"),
    baseHPT= Table[ProgramHeapOp]("pointstocount_hpt")
  )

  // Collections data model + initialization
  case class ProgramHeapCC(x: String, y: String, h: String)
  case class PointsToCC(x: String, y: String)
  def toCollRow1(row: Seq[String]): PointsToCC = PointsToCC(row(0), row(1))
  def toCollRow2(row: Seq[String]): ProgramHeapCC = ProgramHeapCC(row(0), row(1), row(2))
  case class CollectionsDB(newT: Seq[PointsToCC], assign: Seq[PointsToCC], loadT: Seq[ProgramHeapCC], store: Seq[ProgramHeapCC], hpt: Seq[ProgramHeapCC])
  def fromCollRes1(r: PointsToCC): Seq[String] = Seq(
    r.x.toString,
    r.y.toString
  )
  def fromCollRes2(r: ProgramHeapCC): Seq[String] = Seq(
    r.x.toString,
    r.y.toString,
    r.h.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "assign" =>
          loadCSV(csv, toCollRow1)
        case "new" =>
          loadCSV(csv, toCollRow1)
        case "loadT" =>
          loadCSV(csv, toCollRow2)
        case "store" =>
          loadCSV(csv, toCollRow2)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(
      tables("new").asInstanceOf[Seq[PointsToCC]], tables("assign").asInstanceOf[Seq[PointsToCC]],
      tables("loadT").asInstanceOf[Seq[ProgramHeapCC]], tables("store").asInstanceOf[Seq[ProgramHeapCC]],
      Seq()
    )

  //   ScalaSQL data model
  case class ProgramHeapSS[T[_]](x: T[String], y: T[String], h: T[String])
  case class PointsToSS[T[_]](x: T[String], y: T[String])

  def fromSSRes1(r: PointsToSS[?]): Seq[String] = Seq(
    r.x.toString,
    r.y.toString,
  )
  def fromSSRes2(r: ProgramHeapSS[?]): Seq[String] = Seq(
    r.x.toString,
    r.y.toString,
    r.h.toString
  )

  object pointstocount_new extends ScalaSQLTable[PointsToSS]
  object pointstocount_assign extends ScalaSQLTable[PointsToSS]
  object pointstocount_loadT extends ScalaSQLTable[ProgramHeapSS]
  object pointstocount_store extends ScalaSQLTable[ProgramHeapSS]
  object pointstocount_hpt extends ScalaSQLTable[ProgramHeapSS]

  object pointstocount_delta1 extends ScalaSQLTable[PointsToSS]
  object pointstocount_derived1 extends ScalaSQLTable[PointsToSS]
  object pointstocount_tmp1 extends ScalaSQLTable[PointsToSS]
  object pointstocount_delta2 extends ScalaSQLTable[ProgramHeapSS]
  object pointstocount_derived2 extends ScalaSQLTable[ProgramHeapSS]
  object pointstocount_tmp2 extends ScalaSQLTable[ProgramHeapSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[Expr[Int]] = null
  var resultCollections: Seq[Int] = null
//  var resultScalaSQL: Seq[PointsToSS[?]] = null
//  var resultCollections: Seq[PointsToCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val baseVPT = tyqlDB.newT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = tyqlDB.baseHPT
    val pt = unrestrictedFix((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
      val vpt = tyqlDB.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      ).union(
        tyqlDB.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 =>
                (x = l.x, y = hpt.h).toRow
              )
          )
        )
      )
      val hpt = tyqlDB.store.flatMap(s =>
        varPointsTo.flatMap(vpt1 =>
          varPointsTo
            .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 =>
              (x = vpt1.y, y = s.y, h = vpt2.y).toRow
            )
        )
      )

      (vpt, hpt)
    )
    val query = pt._1.filter(vpt => vpt.x == "r").size

    val queryStr = query.toQueryIR.toSQLString().replace("\"", "'")
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val baseVPT = collectionsDB.newT.map(a => PointsToCC(x = a.x, y = a.y))
    val baseHPT = collectionsDB.hpt
    var it = 0
    val pt = FixedPointQuery.multiFix(set)((baseVPT, baseHPT), (Seq(), Seq()))((recur, acc) =>
      val (varPointsTo, heapPointsTo) = recur
      val (varPointsToAcc, heapPointsToAcc) = if it == 0 then (baseVPT, baseHPT) else acc
      it += 1
      val vpt = collectionsDB.assign.flatMap(a =>
        if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
        varPointsTo.filter(p =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          a.y == p.x).map(p =>
          PointsToCC(x = a.x, y = p.y)
        )
      ).union(
        collectionsDB.loadT.flatMap(l =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          heapPointsToAcc.flatMap(hpt =>
            if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
            varPointsTo
              .filter(vpt =>
                if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
                l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 =>
                if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
                PointsToCC(x = l.x, y = hpt.h)
              )
          )
        )
      )
      val hpt = collectionsDB.store.flatMap(s =>
        if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
        varPointsToAcc.flatMap(vpt1 =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          varPointsToAcc
            .filter(vpt2 =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              ProgramHeapCC(x = vpt1.y, y = s.y, h = vpt2.y)
            )
        )
      )

      (vpt, hpt)
    )
    resultCollections = Seq(pt._1.filter(vpt => vpt.x == "r").size)

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    val initBase = () =>
      (pointstocount_new.select.map(c => (c.x, c.y)),
        pointstocount_hpt.select.map(c => (c.x, c.y, c.h)))

    var it = 0
    val fixFn: ((ScalaSQLTable[PointsToSS], ScalaSQLTable[ProgramHeapSS])) => (query.Select[(Expr[String], Expr[String]), (String, String)], query.Select[(Expr[String], Expr[String], Expr[String]), (String, String, String)]) =
      recur =>
        val (varPointsTo, heapPointsTo) = recur
        val (varPointsToAcc, heapPointsToAcc) = if it == 0 then (pointstocount_delta1, pointstocount_delta2) else (pointstocount_derived1, pointstocount_derived2)
        it += 1
        val vpt1 = for {
          a <- pointstocount_assign.select
          p <- varPointsTo.crossJoin()
          if a.y === p.x
        } yield  (a.x, p.y)
        val vpt2 = for {
          l <- pointstocount_loadT.select
          hpt <- heapPointsToAcc.crossJoin()
          vpt <- varPointsTo.crossJoin()
          if l.y === vpt.x && l.h === hpt.y && vpt.y === hpt.x
        } yield (l.x, hpt.h)
        val vpt = vpt1.union(vpt2)

        val hpt = for {
          s <- pointstocount_store.select
          vpt1 <- varPointsToAcc.crossJoin()
          vpt2 <- varPointsToAcc.crossJoin()
          if s.x === vpt1.x && s.h === vpt2.x
        } yield (vpt1.y, s.y, vpt2.y)

        (vpt, hpt)

    FixedPointQuery.scalaSQLSemiNaiveTWO(set)(
      db, (pointstocount_delta1, pointstocount_delta2), (pointstocount_tmp1, pointstocount_tmp2), (pointstocount_derived1, pointstocount_derived2)
    )(
      ((c: PointsToSS[?]) => (c.x, c.y), (c: ProgramHeapSS[?]) => (c.x, c.y, c.h))
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[((ScalaSQLTable[PointsToSS], ScalaSQLTable[ProgramHeapSS])) => (query.Select[Any, Any], query.Select[Any, Any])])

//    val result = pointstocount_derived2.select.filter(_.x === "r").size // this does not work!!!!!
//    println(s"FINAL RES=${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(pointstocount_derived1)} as r ORDER BY r.x")}")
    backupResultScalaSql = ddb.runQuery(s"SELECT COUNT(1) FROM ${ScalaSQLTable.name(pointstocount_derived2)} as r WHERE r.x = 'r'")

//    backupResultScalaSql = ddb.runQuery(s"SELECT * FROM ${ScalaSQLTable.name(pointstocount_derived1)} as r ORDER BY x, y")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("count(1)"), x => Seq(x.toString))

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("count(1)"), x => Seq(x.toString))

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
