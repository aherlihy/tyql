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
import tyql.Query.{fix, unrestrictedFix, unrestrictedBagFix}
import tyql.Expr.{IntLit, StringLit, min, sum}
import Helpers.*

@experimental
class TOJavaPointsTo extends QueryBenchmark {
  override def name = "javapointsto"
  private val outputHeader = Seq("x", "y")
  override def set = true

  // TYQL data model
  type ProgramHeapOp = (x: String, y: String, h: String)
  type ProgramOp = (x: String, y: String)
  type PointsToDB = (newT: ProgramOp, assign: ProgramOp, loadT: ProgramHeapOp, store: ProgramHeapOp, baseHPT: ProgramHeapOp)
  val tyqlDB = (
    newT = Table[ProgramOp]("javapointsto_new"),
    assign = Table[ProgramOp]("javapointsto_assign"),
    loadT = Table[ProgramHeapOp]("javapointsto_loadT"),
    store = Table[ProgramHeapOp]("javapointsto_store"),
    baseHPT= Table[ProgramHeapOp]("javapointsto_hpt")
  )

  // Collections data model + initialization
  case class ProgramHeapCC(x: String, y: String, h: String)
  case class PointsToCC(x: String, y: String)
  def toCollRow1(row: Seq[String]): PointsToCC = PointsToCC(row(0), row(1))
  def toCollRow2(row: Seq[String]): ProgramHeapCC = ProgramHeapCC(row(0), row(1), row(2))
  case class CollectionsDB(newT: Seq[PointsToCC], assign: Seq[PointsToCC], loadT: Seq[ProgramHeapCC], store: Seq[ProgramHeapCC])
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
      tables("loadT").asInstanceOf[Seq[ProgramHeapCC]], tables("store").asInstanceOf[Seq[ProgramHeapCC]]
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

  object javapointsto_new extends ScalaSQLTable[PointsToSS]
  object javapointsto_assign extends ScalaSQLTable[PointsToSS]
  object javapointsto_loadT extends ScalaSQLTable[ProgramHeapSS]
  object javapointsto_store extends ScalaSQLTable[ProgramHeapSS]
  object javapointsto_hpt extends ScalaSQLTable[ProgramHeapSS]

  object javapointsto_delta1 extends ScalaSQLTable[PointsToSS]
  object javapointsto_derived1 extends ScalaSQLTable[PointsToSS]
  object javapointsto_tmp1 extends ScalaSQLTable[PointsToSS]
  object javapointsto_delta2 extends ScalaSQLTable[ProgramHeapSS]
  object javapointsto_derived2 extends ScalaSQLTable[ProgramHeapSS]
  object javapointsto_tmp2 extends ScalaSQLTable[ProgramHeapSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultJDBC_RSQL: ResultSet = null
  var resultJDBC_SNE: ResultSet = null
//  var resultScalaSQL: Seq[ProgramHeapSS[?]] = null
//  var resultCollections: Seq[ProgramHeapCC] = null
  var resultScalaSQL: Seq[PointsToSS[?]] = null
  var resultCollections: Seq[PointsToCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT javapointsto_new2.x as x, javapointsto_new2.y as y FROM javapointsto_new as javapointsto_new2) UNION ((SELECT javapointsto_assign4.x as x, ref2.y as y FROM javapointsto_assign as javapointsto_assign4, recursive1 as ref2 WHERE javapointsto_assign4.y = ref2.x) UNION (SELECT javapointsto_loadT7.x as x, ref5.h as y FROM javapointsto_loadT as javapointsto_loadT7, recursive2 as ref5, recursive1 as ref6 WHERE javapointsto_loadT7.y = ref6.x AND javapointsto_loadT7.h = ref5.y AND ref6.y = ref5.x))),\nrecursive2 AS ((SELECT * FROM javapointsto_hpt as javapointsto_hpt13) UNION ((SELECT ref9.y as x, javapointsto_store15.y as y, ref10.y as h FROM javapointsto_store as javapointsto_store15, recursive1 as ref9, recursive1 as ref10 WHERE javapointsto_store15.x = ref9.x AND javapointsto_store15.h = ref10.x)))\n SELECT * FROM recursive1 as recref0 ORDER BY y ASC, x ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

  def executeTyQL(ddb: DuckDBBackend): Unit =
    val baseVPT = tyqlDB.newT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = tyqlDB.baseHPT

    val tyqlFix = if set then unrestrictedFix((baseVPT, baseHPT)) else unrestrictedBagFix((baseVPT, baseHPT))
    val pt = tyqlFix((varPointsTo, heapPointsTo) => {
      val vpt1 = tyqlDB.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      )
      val vpt2 =
        tyqlDB.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 =>
                (x = l.x, y = hpt.h).toRow
              )
          )
        )
      val vpt = if set then vpt1.union(vpt2) else vpt1.unionAll(vpt2)
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
    })
//    val query = pt._2.sort(_.x, Ord.ASC).sort(_.y, Ord.ASC)
    val query = pt._1.sort(_.x, Ord.ASC).sort(_.y, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0
    val baseVPT = collectionsDB.newT.map(a => PointsToCC(x = a.x, y = a.y))
    val baseHPT = Seq[ProgramHeapCC]()
    val pt = FixedPointQuery.multiFix(set)((baseVPT, baseHPT), (Seq(), Seq()))((recur, acc) =>
      val (varPointsTo, heapPointsTo) = recur
      val (varPointsToAcc, heapPointsToAcc) = if (it == 0) (baseVPT, baseHPT) else acc
      it += 1
      val vpt1 = collectionsDB.assign.flatMap(a =>
        if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
        varPointsTo.filter(p =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          a.y == p.x).map(p =>
          PointsToCC(x = a.x, y = p.y)
        )
      )
      val vpt2 =
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
      val vpt = if set then vpt1.union(vpt2) else vpt1 ++ vpt2
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
//    resultCollections = pt._2.sortBy(_.x).sortBy(_.y)
    resultCollections = pt._1.sortBy(_.x).sortBy(_.y)
    println(s"\nIT,$name,collections,$it")


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    val initBase = () =>
      (javapointsto_new.select.map(c => (c.x, c.y)),
        javapointsto_hpt.select.map(c => (c.x, c.y, c.h)))

    val fixFn: ((ScalaSQLTable[PointsToSS], ScalaSQLTable[ProgramHeapSS])) => (query.Select[(Expr[String], Expr[String]), (String, String)], query.Select[(Expr[String], Expr[String], Expr[String]), (String, String, String)]) =
      recur =>
        val (varPointsTo, heapPointsTo) = recur
        val (varPointsToAcc, heapPointsToAcc) = if it == 0 then (javapointsto_delta1, javapointsto_delta2) else (javapointsto_derived1, javapointsto_derived2)
        it += 1
        val vpt1 = for {
          a <- javapointsto_assign.select
          p <- varPointsTo.join(_.x === a.y)
        } yield (a.x, p.y)
        val vpt2 = for {
          l <- javapointsto_loadT.select
          h <- heapPointsToAcc.join(_.y === l.h)
          v <- varPointsTo.join(_.x === l.y)
          if v.y === h.x
        } yield (l.x, h.h)
        val vpt = if (set) vpt1.union(vpt2) else vpt1.unionAll(vpt2)

        val hpt = for {
          s <- javapointsto_store.select
          v1 <- varPointsToAcc.join(_.x === s.x)
          v2 <- varPointsToAcc.join(_.x === s.h)
        } yield (v1.y, s.y, v2.y)

        (vpt, hpt)

    FixedPointQuery.scalaSQLSemiNaiveTWO(set)(
      ddb, (javapointsto_delta1, javapointsto_delta2), (javapointsto_tmp1, javapointsto_tmp2), (javapointsto_derived1, javapointsto_derived2)
    )(
      ((c: PointsToSS[?]) => (c.x, c.y), (c: ProgramHeapSS[?]) => (c.x, c.y, c.h))
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[((ScalaSQLTable[PointsToSS], ScalaSQLTable[ProgramHeapSS])) => (query.Select[Any, Any], query.Select[Any, Any])])

//    backupResultScalaSql = ddb.runQuery(s"SELECT * FROM ${ScalaSQLTable.name(javapointsto_derived2)} as r ORDER BY r.x, r.y")
    backupResultScalaSql = ddb.runQuery(s"SELECT * FROM ${ScalaSQLTable.name(javapointsto_derived1)} as r ORDER BY r.y, r.x")
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
          collectionToCSV(resultScalaSQL, outfile, outputHeader, fromSSRes1)
      case QueryMode.Collections =>
        val outfile = s"$outdir/collections.csv"
        collectionToCSV(resultCollections, outfile, outputHeader, fromCollRes1)
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
